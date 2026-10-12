"""Источник суточного пакета через обход сквозного счётчика сессий.

Перечисления звонков в API NCC нет отдельным эндпойнтом — это установлено и
записано ([reference/2026-09-12-ncc-integration-pattern.md]). Но идентификатор
сессии устроен как сквозной счётчик:

    node_domain_<domain>_nauss_<nauss>_<epoch>_<seq>

`epoch` постоянна в пределах поколения нумерации, `seq` растёт на единицу с
каждым звонком по всему кластеру, а `domain`/`nauss` говорят, какое плечо звонок
обработало. Проверено на живой площадке 2026-09-12:

* `HEAD /callrecord/download?sessionId=` подтверждает существование сессии и
  отдаёт `Last-Modified` — момент, когда запись дописана, то есть время звонка с
  точностью до конца разговора, — **не скачивая** тела;
* у каждого `seq` ровно один существующий `domain`;
* даты растут вместе с `seq`, но с дрожанием в минуты (`seq` присваивается в
  начале звонка, `Last-Modified` — в конце, а разговоры разной длины).

Замер 2026-09-13 уточнил главное и снял первое впечатление, будто счётчик
плотный. Он **не плотный**: доступные нам записи идут островами — на одном
непрерывном отрезке в двадцать номеров ноль попаданий, на соседнем в пять —
пять. Счётчик сквозной по всему кластеру — замерено 1,33 номера в секунду,
около **115 000 сессий в сутки**, все арендаторы, — а `200` приходит только на
то, что отдают нам; остальное `404`.

Практическое следствие приятное: чужие записи мы **не качаем** — они не
отдаются вовсе. Цена обхода — дешёвые `HEAD` по чужим номерам, тело при этом не
передаётся. Неприятное: сколько из отданного наше, API не сообщает ничем —
ни оператора, ни проекта в `HEAD` нет, только дата и размер.

И главное неприятное — **цена**. Чтобы найти свои записи, приходится опросить
каждый номер: какие наши, заранее неизвестно. Это около 115 000 номеров на
сутки и до четырёх плеч на каждый — **порядка 460 000 запросов за одни сутки**
данных, минут сорок при восьми одновременных. Для ночного приёма это ещё
терпимо, для бэкфилла за три месяца — сорок миллионов запросов, то есть не
вариант. Перечисление кейсов проекта стоит полтора десятка запросов на сутки;
если появится UUID проекта, жить надо на нём.

Отсюда алгоритм, описанный владельцем: взять известную свежую точку, прикинуть
`seq` на начало нужной даты, пройти счётчик, собрать звонки за сутки. Дрожание в
минуты лечится запасом на границах суток и строгой фильтрацией по дате.

Ограничение честно названо: одна опорная точка покрывает одно поколение
нумерации (одну `epoch`). Если NCC пересоберётся и `epoch` сменится, нужна
свежая опорная точка — это одно значение из настройки, не переписывание кода.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from src.infrastructure.sources.ncc.source import CallRef, RecordingNotAvailable

if TYPE_CHECKING:  # pragma: no cover - только типы, цикла импорта нет
    from src.infrastructure.sources.ncc.rest import NccRestClient, RecordingHead

logger = logging.getLogger(__name__)

#: Плечи звонка, которые перебираются на каждом `seq`. У каждого `seq`
#: существует ровно одно сочетание; порядок — просто порядок перебора.
DEFAULT_DOMAINS: tuple[int, ...] = (0, 1, 2, 3)
DEFAULT_NAUSS: tuple[int, ...] = (0,)

#: После скольких подряд пустых номеров спросить дату впереди.
#:
#: **Не порог остановки.** Считать длинный провал концом суток нельзя: счётчик
#: сквозной по кластеру и идёт со скоростью около 115 000 сессий в сутки, то
#: есть 4–5 тысяч номеров в час. Ночью наших звонков нет, и семь тихих часов —
#: это тридцать тысяч пустых номеров подряд при живых сутках. Любой порог,
#: который переживёт такую паузу, не отличит её от настоящего конца, а любой,
#: который её не переживёт, молча оборвёт день.
#:
#: Поэтому провал не останавливает обход, а поднимает вопрос: смотрим дату у
#: ближайшей записи впереди. Дальше суток — конец, внутри суток — идём дальше.
#: Решает дата, а не счёт пропусков.
GAP_DATE_CHECK: int = 2_000

#: Насколько далеко вперёд заглядывать при таком вопросе.
GAP_LOOKAHEAD: int = 2_000

#: Запас на дрожание дат у границ суток. Звонок с большим `seq` может иметь дату
#: чуть меньше соседа: обход не останавливается на первой «завтрашней» дате, а
#: идёт ещё этот запас, и строгая фильтрация по дате отбирает нужное.
BOUNDARY_MARGIN: timedelta = timedelta(minutes=30)

#: Потолок числа проб на одни сутки — предохранитель от бесконечного обхода при
#: неверной опорной точке или сломавшейся нумерации.
#:
#: Счётчик сквозной по **всему кластеру**: замерено около 115 000 сессий в
#: сутки, а не полторы тысячи нашего проекта. Чтобы найти свои, приходится
#: опросить каждый номер — до четырёх плеч на каждый, порядка 460 000 запросов
#: на сутки данных.
#:
#: Потолок стоит выше этого, чтобы честные сутки не обрывались. Упор в него —
#: сигнал, что обход пошёл не туда либо что способ вообще не тот: перечисление
#: кейсов проекта отдаёт только наши звонки и стоит полтора десятка запросов.
MAX_PROBES_PER_DAY: int = 400_000

#: Сколько проб идёт одновременно.
CONCURRENCY: int = 8

#: Радиус локального поиска существующей записи вокруг намеченного номера.
#:
#: Записи идут островами, поэтому «попробовать ровно этот номер» почти всегда
#: промах. Радиус должен покрывать типичный остров пустоты; сотни номеров при
#: наблюдаемой плотности — это единицы найденных записей, то есть достаточно.
SEEK_RADIUS: int = 400

#: Сколько шагов даётся поиску даты, прежде чем признать, что не сходится.
#: Каждый шаг делит промах примерно на порядок, так что дюжины хватает с
#: запасом даже при старой опорной точке.
MAX_SEEK_STEPS: int = 12

#: Насколько раньше начала суток разрешено остановиться поиску. Сбор всё равно
#: идёт вверх и фильтрует по дате, поэтому небольшой недолёт безвреден — он лишь
#: добавляет проб. Большой недолёт при островной раскладке стоит дорого.
SEEK_TOLERANCE: timedelta = timedelta(hours=2)

#: Насколько далеко вниз от прикидки искать данные, прежде чем признать, что
#: запрошенные сутки лежат вне того, что покрывает эта опорная точка. Без
#: потолка запрос суток за пределами диапазона (будущее или до начала истории)
#: сползал бы по одному `seq` через десятки тысяч пустых номеров. В пределах
#: диапазона прикидка попадает рядом с данными, и этот спуск не нужен вовсе.
MAX_OUTSIDE_SCAN: int = 5_000

_ANCHOR_RE = re.compile(
    r"^node(?:_(?P<node>\d+))?_domain_\d+_nauss_\d+_(?P<epoch>\d+)_(?P<seq>\d+)$"
)


class SequenceNotConfigured(RuntimeError):
    """Нет опорной точки — обходить счётчик не от чего."""


class SessionTemplate(BaseModel):
    """Форма идентификатора сессии, из которой строятся кандидаты.

    Постоянное — префикс узла (в новой форме его нет вовсе) и `epoch`. Меняется
    на каждом звонке — `domain`, `nauss`, `seq`.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    node: str | None
    epoch: str

    @classmethod
    def parse(cls, session_id: str) -> tuple[SessionTemplate, int]:
        """Разобрать опорный идентификатор в шаблон и его `seq`."""
        match = _ANCHOR_RE.match(session_id.strip())
        if match is None:
            raise SequenceNotConfigured(f"опорный идентификатор сессии не разобран: {session_id!r}")
        return cls(node=match.group("node"), epoch=match.group("epoch")), int(match.group("seq"))

    def build(self, *, domain: int, nauss: int, seq: int) -> str:
        """Собрать проверенный запрос последовательного источника."""
        head = "node" if self.node is None else f"node_{self.node}"
        return f"{head}_domain_{domain}_nauss_{nauss}_{self.epoch}_{seq}"


class _Hit(BaseModel):
    """Существующая сессия, найденная на одном `seq`."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    session_id: str
    seq: int
    domain: int
    nauss: int
    modified: datetime | None


class SequenceWalkSource:
    """Звонки за сутки — обходом сквозного счётчика сессий.

    Реализует тот же порт `DailyBatchSource`, что и остальные источники:
    `list_calls(day)` и `open_audio(ref)`. Внутри — обход `seq` с `HEAD` для
    даты, снаружи — обычный источник, который приёму безразлично, откуда берёт.
    """

    name = "ncc_sequence"

    def __init__(
        self,
        client: NccRestClient,
        *,
        anchor_session_id: str,
        domains: Sequence[int] = DEFAULT_DOMAINS,
        nauss_values: Sequence[int] = DEFAULT_NAUSS,
        timezone: str,
    ) -> None:
        """Настроить SequenceWalkSource из переданных параметров и зависимостей."""
        if not anchor_session_id.strip():
            raise SequenceNotConfigured(
                "SPEECH_NCC_SOURCE_KIND=sequence требует SPEECH_NCC_SESSION_ANCHOR — "
                "недавний идентификатор сессии. Взять его негде, кроме как из "
                "настоящего звонка: перечисления сессий в API нет."
            )
        self._client = client
        self._template, self._anchor_seq = SessionTemplate.parse(anchor_session_id)
        self._domains = tuple(domains) or DEFAULT_DOMAINS
        self._nauss = tuple(nauss_values) or DEFAULT_NAUSS
        self._tz = ZoneInfo(timezone)
        self._sem = asyncio.Semaphore(CONCURRENCY)
        #: Локальность: соседние `seq` часто делит одно плечо, поэтому удачное
        #: пробуется первым — обычно этого хватает и остальные плечи не трогаем.
        self._last_domain = self._domains[0]
        self._last_nauss = self._nauss[0]
        #: Кэш опорной даты и скорости счётчика; считаются один раз.
        self._anchor_dt: datetime | None = None
        self._rate: float | None = None

    # ------------------------------------------------------------------ #
    # Проба одного seq
    # ------------------------------------------------------------------ #

    async def _probe(self, seq: int) -> _Hit | None:
        """Найти существующую сессию на этом `seq`, перебрав плечи.

        Возвращает `None`, если ни одно сочетание не существует, — это дырка в
        нумерации, а не ошибка. Отказ сети поднимается исключением: обход не
        должен принять сбой за конец звонков.
        """
        order = self._combo_order()
        for domain, nauss in order:
            head = await self._head(domain, nauss, seq)
            if head.exists:
                self._last_domain, self._last_nauss = domain, nauss
                return _Hit(
                    session_id=self._template.build(domain=domain, nauss=nauss, seq=seq),
                    seq=seq,
                    domain=domain,
                    nauss=nauss,
                    modified=self._localise(head.modified),
                )
        return None

    async def _head(self, domain: int, nauss: int, seq: int) -> RecordingHead:
        """Прочитать метаданные текущей страницы последовательного источника."""
        session_id = self._template.build(domain=domain, nauss=nauss, seq=seq)
        async with self._sem:
            return await self._client.head_recording(session_id)

    def _combo_order(self) -> list[tuple[int, int]]:
        """Сочетания плеч, удачное прошлое — первым."""
        combos = [(d, n) for n in self._nauss for d in self._domains]
        preferred = (self._last_domain, self._last_nauss)
        combos.sort(key=lambda c: c != preferred)
        return combos

    def _localise(self, moment: datetime | None) -> datetime | None:
        """Привести дату источника к явному часовому поясу."""
        if moment is None:
            return None
        return moment.astimezone(self._tz)

    # ------------------------------------------------------------------ #
    # Опорная точка и скорость счётчика
    # ------------------------------------------------------------------ #

    async def _anchor(self) -> tuple[int, datetime]:
        """Опорный `seq` и его дата. Считается один раз."""
        if self._anchor_dt is None:
            hit = await self._probe(self._anchor_seq)
            if hit is None or hit.modified is None:
                raise SequenceNotConfigured(
                    f"опорная сессия {self._anchor_seq} не найдена или без даты — "
                    "нужна свежая SPEECH_NCC_SESSION_ANCHOR"
                )
            self._anchor_dt = hit.modified
        return self._anchor_seq, self._anchor_dt

    async def _seqs_per_second(self) -> float:
        """Скорость счётчика: сколько `seq` приходится на секунду.

        Две точки — опорная и одна ниже неё. Вторая ищется точечно, в нескольких
        местах на разном отдалении, а не линейным спуском: записи идут островами,
        и спуск по одному номеру пробивался бы сквозь пустоту тысячами запросов
        ради одного числа, которое считается один раз за прогон.

        Отдаления разные, потому что скорость счётчика неравномерна (день, ночь):
        близкая точка даёт местную скорость, дальняя — среднюю.
        """
        if self._rate is not None:
            return self._rate
        anchor_seq, anchor_dt = await self._anchor()
        sample: tuple[int, datetime] | None = None
        for back in (2_000, 500, 10_000, 50_000):
            sample = await self._nearest_dated(anchor_seq - back)
            if sample is not None:
                break
        if sample is None:
            # Не с чем сравнить — берём осторожное умолчание: один звонок в
            # минуту. Хуже точной оценки, но обход всё равно найдёт край по
            # датам, просто с большим первым перелётом.
            self._rate = 1.0 / 60.0
            return self._rate
        seq_b, dt_b = sample
        span = (anchor_dt - dt_b).total_seconds()
        self._rate = (anchor_seq - seq_b) / span if span > 0 else 1.0 / 60.0
        return self._rate

    async def _first_datable_toward_anchor(self, start: int) -> tuple[int, datetime] | None:
        """Ближайший датируемый звонок в сторону опорной точки.

        Направление здесь существенно. Прикидка по скорости счётчика
        промахивается в обе стороны, и искать всегда вниз — значит при недолёте
        **ниже** данных уходить от них, а не к ним. Опорная точка — единственный
        `seq`, о котором точно известно, что он существует, поэтому идти надо к
        ней: данные заведомо лежат на этом отрезке.
        """
        if start == self._anchor_seq:
            return await self._nearest_dated(start)
        step = 1 if self._anchor_seq > start else -1
        seq = start
        for _ in range(MAX_OUTSIDE_SCAN):
            if seq <= 0:
                break
            hit = await self._probe(seq)
            if hit is not None and hit.modified is not None:
                return hit.seq, hit.modified
            if seq == self._anchor_seq:
                break
            seq += step
        return None

    # ------------------------------------------------------------------ #
    # Порт источника
    # ------------------------------------------------------------------ #

    async def list_calls(self, day: date) -> Sequence[CallRef]:
        """Все звонки за указанную дату.

        Границы суток — в поясе колл-центра: «за такое-то число» для человека
        это его число, а в UTC сутки съезжают на три часа. Пустой результат —
        законный ответ (выходной), он не отличается от ошибки на уровне типа:
        ошибка обязана быть исключением.
        """
        start = datetime.combine(day, time.min, tzinfo=self._tz)
        end = start + timedelta(days=1)

        lower = await self._find_lower_edge(start)
        if lower is None:
            # Сутки не нашлись в счётчике: либо они вне того, что покрывает эта
            # опорная точка, либо опорная точка устарела. Пусто — но сказанное
            # вслух: «звонков не было» и «мы не нашли, где искать» снаружи
            # выглядят одинаково, а означают разное.
            logger.warning(
                "ncc sequence: сутки %s не найдены в счётчике от опорной точки %s — "
                "возможно, нужна свежая SPEECH_NCC_SESSION_ANCHOR",
                start.date().isoformat(),
                self._anchor_seq,
            )
            return ()
        return await self._collect(lower, start, end)

    async def _find_lower_edge(self, start: datetime) -> int | None:
        """`seq`, заведомо чуть раньше начала суток, либо `None`.

        Прикидка по скорости счётчика, затем спуск, пока дата не уйдёт за начало
        суток с запасом на дрожание. Возвращается `seq`, с которого безопасно
        начинать сбор вверх.

        `None` — данных рядом с прикидкой нет вовсе. Идти «вверх на всякий
        случай» в этом случае незачем: это тысячи пустых проб ради заведомо
        пустого ответа.
        """
        anchor_seq, anchor_dt = await self._anchor()
        rate = await self._seqs_per_second()
        target = start - BOUNDARY_MARGIN

        # Поиск итеративный, а не «одна прикидка и спуск». Одна прикидка верна
        # ровно настолько, насколько верна средняя скорость счётчика, а та
        # неравномерна (день/ночь). На опорной точке недельной давности промах
        # достигает сотен тысяч номеров — и при островной раскладке локальный
        # поиск в него просто не попадает, то есть сутки «не находятся» вовсе.
        #
        # Здесь каждый шаг смотрит на дату найденного номера и прыгает на
        # расстояние, посчитанное **от неё**. Промах делится примерно на порядок
        # за шаг, поэтому одной опорной точки хватает на любую дату внутри
        # поколения нумерации — даже сильно от неё удалённую.
        seq, moment = anchor_seq, anchor_dt
        for _ in range(MAX_SEEK_STEPS):
            drift = (moment - target).total_seconds()
            if -SEEK_TOLERANCE.total_seconds() <= drift <= 0:
                break
            jump = int(rate * drift) or (1 if drift > 0 else -1)
            probed = await self._nearest_dated(max(seq - jump, 1), radius=SEEK_RADIUS)
            if probed is None:
                # Рядом с намеченным местом данных нет: либо сутки вне того, что
                # покрывает эта опорная точка, либо нумерация там кончилась.
                return None
            seq, moment = probed

        # Добор вниз: сбор идёт вверх, поэтому начинать надо заведомо раньше
        # начала суток, иначе первые звонки дня останутся ниже старта.
        descent = max(int(rate * BOUNDARY_MARGIN.total_seconds()), SEEK_RADIUS)
        while moment > target and seq > 1:
            probed = await self._nearest_dated(max(seq - descent, 1), radius=SEEK_RADIUS)
            if probed is None:
                break
            seq, moment = probed
        return seq

    async def _collect(self, lower: int, start: datetime, end: datetime) -> tuple[CallRef, ...]:
        """Идти вверх от нижней точки, собирая звонки за сутки."""
        refs: dict[str, CallRef] = {}
        seq = lower
        gaps = 0
        probes = 0
        past_end_margin = end + BOUNDARY_MARGIN
        seen_day = False

        while probes < MAX_PROBES_PER_DAY:
            batch = list(range(seq, seq + CONCURRENCY))
            probes += len(batch)
            hits = await asyncio.gather(*(self._probe(s) for s in batch))
            stop = False
            for hit in hits:
                if hit is None:
                    gaps += 1
                    continue
                gaps = 0
                if hit.modified is None:
                    # Существует, но недатируем: относим к текущим суткам, а не
                    # теряем. На практике не встречается — сервер всегда шлёт
                    # дату, — но потерять реальный звонок хуже, чем взять лишний.
                    refs[hit.session_id] = self._ref(hit, day_start=start)
                    continue
                if hit.modified >= past_end_margin:
                    # Заведомо за сутками с запасом на дрожание — дальше только
                    # позже. Останавливаемся.
                    stop = True
                    continue
                if start <= hit.modified < end:
                    seen_day = True
                    refs[hit.session_id] = self._ref(hit, day_start=start)
                # Раньше суток (в пределах запаса) — пропускаем, идём выше.
            if not stop and gaps >= GAP_DATE_CHECK:
                # Длинный провал: спрашиваем дату впереди, а не считаем пропуски.
                # Ночную тишину и конец суток по числу пустых номеров различить
                # нельзя, по дате — можно.
                ahead = await self._nearest_dated(
                    seq + CONCURRENCY + GAP_LOOKAHEAD, radius=SEEK_RADIUS
                )
                if ahead is None or ahead[1] >= past_end_margin:
                    stop = True
                else:
                    gaps = 0
            if stop:
                break
            seq += CONCURRENCY
        else:
            # Цикл вышел по потолку проб, а не по краю данных: сутки могли
            # оборваться недобранными. Молчать нельзя — «собрано N» тогда
            # означает «N и, возможно, ещё сколько-то».
            logger.warning(
                "ncc sequence: обход %s упёрся в потолок %s проб — сутки могли "
                "недобраться; счётчик сквозной по кластеру, проверьте способ сбора",
                start.date().isoformat(),
                MAX_PROBES_PER_DAY,
            )

        if not seen_day and refs:
            logger.info(
                "ncc sequence: за %s собрано %s звонков только по недатируемым сессиям",
                start.date().isoformat(),
                len(refs),
            )
        return tuple(refs.values())

    def _ref(self, hit: _Hit, *, day_start: datetime) -> CallRef:
        """Преобразовать строку источника в локатор и метаданные звонка."""
        return CallRef(
            session_id=hit.session_id,
            # Локатор и есть идентификатор: скачивание идёт по нему.
            audio_locator=hit.session_id,
            day=(hit.modified.date() if hit.modified is not None else day_start.date()),
            started_at=hit.modified,
        )

    async def _nearest_dated(self, seq: int, *, radius: int = 25) -> tuple[int, datetime] | None:
        """Ближайший к `seq` существующий и датируемый звонок.

        Записи идут островами, поэтому попасть точно в номер почти нельзя;
        ищем расходящимся кольцом от намеченного места. Радиус задаётся
        вызывающим: при замере скорости хватает малого, при поиске даты нужен
        такой, чтобы перекрыть типичный остров пустоты.
        """
        for offset in range(0, max(radius, 1)):
            for candidate in {seq + offset, seq - offset} if offset else {seq}:
                if candidate <= 0:
                    continue
                hit = await self._probe(candidate)
                if hit is not None and hit.modified is not None:
                    return hit.seq, hit.modified
        return None

    async def open_audio(self, ref: CallRef) -> AsyncIterator[bytes]:
        """Запись по идентификатору сессии.

        `RecordingNotAvailable` из клиента проходит наверх как есть: отличать
        «записи нет» от «источник сломан» — обязанность этапа скачивания.
        """
        return self._client.download(ref.session_id)


# Подсказка для проверки типов: `RecordingNotAvailable` переиспользуется
# вызывающими из этого модуля.
__all__ = [
    "SequenceWalkSource",
    "SessionTemplate",
    "SequenceNotConfigured",
    "RecordingNotAvailable",
]
