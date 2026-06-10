"""События модуля Office — связи с другими отделами.

Офис-менеджер сидит в центре постпродажного контура и связан с пятью отделами:

    CRM/Sales ──sales.deal.won──▶ ОФИС ──office.shipment.requested──▶ Склад
                                    │
                                    ├──logistics.delivery.requested──▶ Логистика  (заявка перевозчику)
                                    ├──office.docs.collected─────────▶ Финансы
                                    ├──office.payment.awaiting────────▶ Финансы    (+ approval при сумме > порога)
                                    └──office.payment.overdue─────────▶ Юрист      (претензия при просрочке)

    Склад ──wms.shipment.completed──▶ ОФИС   (двигаем в «Отгружено»)
    Логистика ──logistics.delivery.delivered──▶ ОФИС   (трекинг доставки)
    Финансы ──finance.payment.received──▶ ОФИС   (двигаем в «Оплачено»)

Шина — transactional outbox (``core/services/eventbus.py``): ``emit`` пишет событие
в той же транзакции, доставка подписчикам — relay (at-least-once). Обработчики с
двумя параметрами получают ``ctx`` (сессия + фасад сервисов), см. ``EventContext``.

AI-маршрутизация событий (приоритезация, авто-claims, прогноз оплаты) — Итерация 1.
Здесь — детерминированная проводка без AI.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.office.models import OfficeDoc

logger = logging.getLogger("aios.office")

# Порог суммы (BYN), выше которого оплата требует согласования (флаг для Финансов).
APPROVAL_THRESHOLD = Decimal("10000")
# Порог просрочки (дней), после которого долг уходит Юристу на претензию.
OVERDUE_CLAIM_DAYS = 15


# --------------------------------------------------------------------------- #
#  Исходящие события (emit-хелперы) — вызываются из routes.py и обработчиков.
# --------------------------------------------------------------------------- #
def _doc_payload(doc: OfficeDoc, **extra: Any) -> dict:
    """Единый снимок документа для события (то, что нужно соседним отделам)."""
    payload = {
        "doc_id": doc.id,
        "number": doc.number,
        "company": doc.company,
        "title": doc.title,
        "amount": float(doc.amount or 0),
        "stage": doc.stage,
        "owner": doc.owner,
        "region": doc.region,
        "weight": doc.weight,
        "address": doc.address,
        "sales_ref": doc.sales_ref,
        "entity_ref": doc.number or f"office:{doc.id}",
    }
    payload.update(extra)
    return payload


def emit_doc_created(bus, session: AsyncSession, doc: OfficeDoc) -> None:
    """Документ заведён в офисе (для аудита/дашборда владельца)."""
    bus.emit(session, "office.doc.created", _doc_payload(doc))


def emit_shipment_requested(bus, session: AsyncSession, doc: OfficeDoc) -> None:
    """→ Склад: собрать и подготовить заказ к отгрузке."""
    bus.emit(session, "office.shipment.requested", _doc_payload(doc))


def emit_carrier_request(
    bus,
    session: AsyncSession,
    doc: OfficeDoc,
    *,
    log_ref: str,
    carrier: str,
    carrier_name: str,
    region: str,
    pickup_date: str = "",
    contact: str = "",
    comment: str = "",
) -> None:
    """→ Логистика: заявка перевозчику на доставку по РБ (кнопка из карточки)."""
    bus.emit(
        session,
        "logistics.delivery.requested",
        _doc_payload(
            doc,
            log_ref=log_ref,
            carrier=carrier,
            carrier_name=carrier_name,
            region=region or doc.region,
            pickup_date=pickup_date,
            contact=contact,
            comment=comment,
            entity_ref=log_ref,
        ),
    )


def emit_docs_collected(bus, session: AsyncSession, doc: OfficeDoc) -> None:
    """→ Финансы: пакет документов собран, можно выставлять/ждать оплату."""
    bus.emit(session, "office.docs.collected", _doc_payload(doc))


def emit_payment_awaiting(bus, session: AsyncSession, doc: OfficeDoc) -> None:
    """→ Финансы: ожидаем оплату. Крупная сумма помечается на согласование."""
    needs_approval = Decimal(str(doc.amount or 0)) > APPROVAL_THRESHOLD
    bus.emit(
        session,
        "office.payment.awaiting",
        _doc_payload(doc, needs_approval=needs_approval, threshold=float(APPROVAL_THRESHOLD)),
    )


def emit_payment_overdue(bus, session: AsyncSession, doc: OfficeDoc) -> None:
    """→ Юрист: дебиторка просрочена сверх порога — основание для претензии."""
    bus.emit(
        session,
        "office.payment.overdue",
        _doc_payload(doc, overdue_days=doc.overdue_days, claim_days=OVERDUE_CLAIM_DAYS),
    )


# --------------------------------------------------------------------------- #
#  Входящие события (обработчики) — регистрируются в module.register().
# --------------------------------------------------------------------------- #
def _first(payload: dict, *keys: str, default: str = "") -> str:
    """Достать первое непустое значение из payload по списку возможных ключей."""
    for k in keys:
        v = payload.get(k)
        if v not in (None, ""):
            return str(v)
    return default


async def _find_doc(session: AsyncSession, **by: str) -> OfficeDoc | None:
    """Найти документ по любому из ref-полей (sales/wms/logistics/finance/number)."""
    for field, value in by.items():
        if not value:
            continue
        col = getattr(OfficeDoc, field, None)
        if col is None:
            continue
        row = (
            await session.execute(select(OfficeDoc).where(col == value).limit(1))
        ).scalars().first()
        if row is not None:
            return row
    return None


async def on_deal_won(payload: dict, ctx) -> None:
    """CRM: сделка выиграна → завести документ в стадии «Готово к отгрузке»."""
    session: AsyncSession = ctx.session
    sales_ref = _first(payload, "deal_ref", "number", "deal_id", "entity_ref")

    existing = await _find_doc(session, sales_ref=sales_ref)
    if existing is not None:
        logger.info("office: deal %s уже заведён как %s", sales_ref, existing.number)
        return

    doc = OfficeDoc(
        company=_first(payload, "company", "client", "counterparty"),
        title=_first(payload, "title", "subject", "product"),
        amount=Decimal(str(payload.get("amount") or 0)),
        owner=_first(payload, "owner", "manager", "by"),
        region=_first(payload, "region", "city"),
        weight=_first(payload, "weight"),
        address=_first(payload, "address"),
        sales_ref=sales_ref,
        stage="ready",
        docs_status="Ожидает отгрузки",
        next_step="Передать на склад для сборки",
    )
    session.add(doc)
    await session.flush()
    if not doc.number:
        doc.number = f"ДОК-2026-{doc.id:04d}"

    bus = ctx.services.event_bus
    emit_doc_created(bus, session, doc)
    emit_shipment_requested(bus, session, doc)  # сразу просим Склад собрать заказ
    logger.info("office: создан %s по сделке %s", doc.number, sales_ref)


async def on_shipment_completed(payload: dict, ctx) -> None:
    """Склад: отгрузка собрана → двигаем документ в «Отгружено»."""
    session: AsyncSession = ctx.session
    doc = await _find_doc(
        session,
        wms_ref=_first(payload, "wms_ref", "shipment_ref"),
        sales_ref=_first(payload, "sales_ref", "deal_ref"),
        number=_first(payload, "doc_number", "number"),
    )
    if doc is None:
        logger.warning("office: wms.shipment.completed — документ не найден (%s)", payload)
        return
    doc.wms_ref = _first(payload, "wms_ref", "shipment_ref", default=doc.wms_ref)
    doc.stage = "shipped"
    doc.docs_status = "Отгружено, готовим документы"
    doc.next_step = "Собрать пакет документов (ТТН, счёт-фактура, акт)"


async def on_delivery_delivered(payload: dict, ctx) -> None:
    """Логистика: перевозчик доставил груз → фиксируем трекинг в документе."""
    session: AsyncSession = ctx.session
    doc = await _find_doc(
        session,
        logistics_ref=_first(payload, "log_ref", "logistics_ref"),
        number=_first(payload, "doc_number", "number"),
    )
    if doc is None:
        logger.warning("office: logistics.delivery.delivered — документ не найден (%s)", payload)
        return
    doc.delivery = _first(payload, "carrier_name", "carrier", default=doc.delivery)
    doc.op_date = _first(payload, "delivered_at", "date", default=doc.op_date or "")
    doc.docs_status = "Доставлено, закрываем документы"


async def on_payment_received(payload: dict, ctx) -> None:
    """Финансы: оплата получена → двигаем документ в «Оплачено» и снимаем просрочку."""
    session: AsyncSession = ctx.session
    doc = await _find_doc(
        session,
        finance_ref=_first(payload, "finance_ref", "invoice_ref"),
        sales_ref=_first(payload, "sales_ref", "deal_ref"),
        number=_first(payload, "doc_number", "number"),
    )
    if doc is None:
        logger.warning("office: finance.payment.received — документ не найден (%s)", payload)
        return
    doc.finance_ref = _first(payload, "finance_ref", "invoice_ref", default=doc.finance_ref)
    doc.stage = "paid"
    doc.docs_status = "Оплачено"
    doc.overdue_days = 0
    doc.next_step = "Сделка закрыта"
