from marshmallow import validate
from umongo import Document
from umongo.fields import ObjectIdField, IntegerField

from app import app


instance = app.config["LAZY_UMONGO"]


@instance.register
class PlayerStatistic(Document):
    player_id = ObjectIdField(allow_none=False, required=True, unique=True)
    total_games = IntegerField(
        allow_none=False,
        required=False,
        default=0,
        validate=validate.Range(
            min=0,
            error='Field value cannot be represented by a negative integer value.'
        )
    )
    wins = IntegerField(
        allow_none=False,
        required=False,
        default=0,
        validate=validate.Range(
            min=0,
            error='Field value cannot be represented by a negative integer value.'
        )
    )
    loses = IntegerField(
        allow_none=False,
        required=False,
        default=0,
        validate=validate.Range(
            min=0,
            error='Field value cannot be represented by a negative integer value.'
        )
    )
    rating = IntegerField(
        allow_none=False,
        required=False,
        default=0,
        validate=validate.Range(
            min=0,
            error='Field value cannot be represented by a negative integer value.'
        )
    )

    class Meta:
        strict = False
