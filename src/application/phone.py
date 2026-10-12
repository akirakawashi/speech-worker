"""Производные данные номера для регистрации звонка без хранения исходного номера."""

from src.domain.calls.phone import (
    hash_phone_number as hash_phone_number,
    mask_phone_number as mask_phone_number,
    mask_phones_in_snapshot as mask_phones_in_snapshot,
)
