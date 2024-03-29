class Error(Exception):
    pass


class WebsocketClientError(Error):
    def __init__(self, error_message):
        self.error_message = error_message

    def __str__(self):
        return self.error_message


class RequestLimitExceededError(Error):
    def __init__(self, error_message):
        self.error_message = error_message

    def __str__(self):
        return self.error_message


class NotSuccessStatusError(Error):
    def __init__(self, error_message):
        self.error_message = error_message

    def __str__(self):
        return self.error_message