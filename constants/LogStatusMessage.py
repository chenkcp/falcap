class LogStatusMessage:
    @staticmethod
    def no_email_sent():
        return "NO EMAIL SENT"

    @staticmethod
    def accept():
        return "ACCEPT"

    @staticmethod
    def block():
        return "BLOCK"

    @staticmethod
    def skip():
        return "SKIP"

    @staticmethod
    def constraint_pass(constraint_key, reason=None):
        msg = f"PASS - {constraint_key}"
        if reason:
            msg += f" - {reason}"
        return msg

    @staticmethod
    def constraint_fail(constraint_key, reason=None):
        msg = f"FAIL - {constraint_key}"
        if reason:
            msg += f" - {reason}"
        return msg

    @staticmethod
    def constraint_block(constraint_key, reason=None):
        msg = f"BLOCK - {constraint_key}"
        if reason:
            msg += f" - {reason}"
        return msg

    @staticmethod
    def constraint_bypass(constaint_key, reason=None):
        msg = f"BYPASS - {constaint_key}"
        if reason:
            msg += f" - {reason}"
        return msg

    @staticmethod
    def constraint_skip(constraint_key, reason=None):
        msg = f"SKIP - {constraint_key}"
        if reason:
            msg += f" - {reason}"
        return msg

    @staticmethod
    def test_result(result):
        return f"TEST RESULT - {result}"

    @staticmethod
    def email_sent():
        return "EMAIL SENT"

    @staticmethod
    def test_delta_e(constraint_key):
        return (
            LogStatusMessage.constraint_skip(constraint_key, "No parametric data")
            + " - calc Hue 2 Delta E - update param column"
        )
