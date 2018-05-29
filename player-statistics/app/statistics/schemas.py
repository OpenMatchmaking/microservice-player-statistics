from bson import ObjectId
from marshmallow import Schema, fields, validates, ValidationError

from app import app


PlayerStatistic = app.config["LAZY_UMONGO"].PlayerStatistic


class InitPlayerStatisticSchema(PlayerStatistic.schema.as_marshmallow_schema()):
    id = fields.String(required=False, allow_none=False)

    @validates('id')
    def validate_id(self, value):
        if not ObjectId.is_valid(value):
            raise ValidationError(
                "'{}' is not a valid ObjectId, it must be a 12-byte "
                "input or a 24-character hex string.".format(value)
            )

    class Meta:
        model = PlayerStatistic
        fields = (
            'id',
            'player_id',
        )


class RetrievePlayerStatisticSchema(PlayerStatistic.schema.as_marshmallow_schema()):

    class Meta:
        model = PlayerStatistic
        fields = (
            'player_id',
        )


class UpdatePlayerStatisticSchema(PlayerStatistic.schema.as_marshmallow_schema()):
    IS_DECREASED_ERROR_TEMPLATE = "The passed value='{}' must be greater or equal to the current."

    def validate_for_increased_value(self, old_value, new_value):
        if new_value < old_value:
            raise ValidationError(self.IS_DECREASED_ERROR_TEMPLATE.format(new_value))

    @validates('total_games')
    def validate_total_games_is_increased(self, value):
        self.validate_for_increased_value(self.total_games, value)

    @validates('wins')
    def validate_wins_is_increased(self, value):
        self.validate_for_increased_value(self.wins, value)

    @validates('loses')
    def validate_loses_is_increased(self, value):
        self.validate_for_increased_value(self.loses, value)

    class Meta:
        model = PlayerStatistic
