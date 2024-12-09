import attr


@attr.s
class Message:
    sequence_number = attr.ib()
    message = attr.ib()

    def to_json(self):
        return attr.asdict(self)
