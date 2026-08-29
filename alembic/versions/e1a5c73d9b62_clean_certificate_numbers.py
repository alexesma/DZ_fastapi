"""clean supplier-mangled certificate numbers

В выгрузке поставщика слэш в номере заменён подчёркиванием, а к части
номеров дописан комментарий о том, что документ покрывает. Клиент
проверяет номер в реестре дословно, поэтому такой номер отправлять
нельзя: он там просто не найдётся.

Список правок задан явно, а не регулярным выражением: это номера
официальных документов, и «похожие» строки трогать вслепую нельзя.
Комментарий переносим в scope, чтобы не потерять смысл.

Revision ID: e1a5c73d9b62
Revises: d9f3b6c2a840
Create Date: 2026-08-29 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1a5c73d9b62"
down_revision: Union[str, None] = "d9f3b6c2a840"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (как пришло из файла, как должно быть, что дописал поставщик)
FIXES: tuple[tuple[str, str, Union[str, None]], ...] = (
    (
        "RU С-CN.АД58.В.01479_25_TATSUMI_lamps",
        "RU С-CN.АД58.В.01479/25",
        "TATSUMI lamps",
    ),
    ("RU С-CN.НА72.В.00723_24", "RU С-CN.НА72.В.00723/24", None),
    ("RU С-DE.АД50.В.04954_22", "RU С-DE.АД50.В.04954/22", None),
    ("RU С-DE.АД50.В.06269_23", "RU С-DE.АД50.В.06269/23", None),
    (
        "RU С-DE.НА46.В.06971_23_ремкомплекты суппорта",
        "RU С-DE.НА46.В.06971/23",
        "ремкомплекты суппорта",
    ),
    ("RU С-JP.АД50.В.04932_22", "RU С-JP.АД50.В.04932/22", None),
    ("RU С-JP.АД50.В.05106_22", "RU С-JP.АД50.В.05106/22", None),
    ("RU С-JP.АД50.В.06656_24", "RU С-JP.АД50.В.06656/24", None),
    (
        "RU С-JP.АД50.В.07065_24_Glow plugs_Wiper blades",
        "RU С-JP.АД50.В.07065/24",
        "Glow plugs, Wiper blades",
    ),
    ("RU С-JP.НА46.В.06491_23_TATSUMI", "RU С-JP.НА46.В.06491/23", "TATSUMI"),
    ("ЕАЭС KG417_026.JP.02.20255", "ЕАЭС KG417/026.JP.02.20255", None),
    ("ЕАЭС KG417_039.DE.02.04523", "ЕАЭС KG417/039.DE.02.04523", None),
    ("ЕАЭС KG417_039.JP.02.04978", "ЕАЭС KG417/039.JP.02.04978", None),
)

_RENAME = sa.text(
    """
    UPDATE certificate
       SET number = :clean,
           scope = COALESCE(scope, :scope)
     WHERE number = :dirty
       AND NOT EXISTS (
         SELECT 1 FROM certificate other WHERE other.number = :clean
       )
    """
)


def upgrade() -> None:
    bind = op.get_bind()
    for dirty, clean, scope in FIXES:
        bind.execute(
            _RENAME, {"dirty": dirty, "clean": clean, "scope": scope}
        )

    # Кэш на карточках собран из номеров, поэтому переименование должно
    # доехать и туда — иначе в прайс уйдёт старое написание.
    bind.execute(
        sa.text(
            """
            UPDATE autopart a
               SET eac_cert_number = c.number
              FROM autopart_certificate_association link
              JOIN certificate c ON c.id = link.certificate_id
             WHERE link.autopart_id = a.id
               AND a.eac_cert_number IS DISTINCT FROM c.number
               AND a.eac_cert_number = ANY(:dirty)
            """
        ).bindparams(sa.bindparam("dirty", value=[item[0] for item in FIXES])),
    )


def downgrade() -> None:
    bind = op.get_bind()
    for dirty, clean, _ in FIXES:
        bind.execute(
            sa.text("UPDATE certificate SET number = :dirty WHERE number = :clean"),
            {"dirty": dirty, "clean": clean},
        )
