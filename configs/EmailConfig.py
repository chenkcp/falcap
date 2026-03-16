from exceptions.InvalidConfigException import InvalidConfigException


class EmailConfig:
    def __init__(self, email_config=None):
        if email_config is None:
            raise InvalidConfigException("EmailConfig.__init__(): email_config is None")

        self._email_error_group = email_config["error_group"]
        self._email_pass_group = email_config["pass_group"]
        self._email_fail_group = email_config["fail_group"]
        self._email_rejected_group = email_config["rejected_group"]
        self._smtp_server_host = email_config["smtp_host"]
        self._smtp_server_port = email_config["smtp_port"]
        self._email_sender = email_config["sender"]

    @property
    def email_error_group(self):
        return self._email_error_group

    @property
    def email_pass_group(self):
        return self._email_pass_group

    @property
    def email_fail_group(self):
        return self._email_fail_group

    @property
    def email_rejected_group(self):
        return self._email_rejected_group

    @property
    def smtp_server_host(self):
        return self._smtp_server_host

    @property
    def smtp_server_port(self):
        return self._smtp_server_port

    @property
    def email_sender(self):
        return self._email_sender
