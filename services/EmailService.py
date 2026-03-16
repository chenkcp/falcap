import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class EmailService:
    logger = logging.getLogger(__name__)

    def __init__(self, email_config, falcap_web_url):
        self.logger.info("init EmailService")
        self._email_config = email_config
        self._falcap_web_url = falcap_web_url
        self.pass_html_template_parts = []
        self.error_html_template_parts = []
        self.rejected_html_template_parts = []
        self.fail_html_template_parts = []
        self._server = None
        self.init_smtp()
        self.init_templates()

    def init_smtp(self):
        if self._server is not None:
            self._server.quit()

        self._server = smtplib.SMTP(
            self._email_config.smtp_server_host, self._email_config.smtp_server_port
        )

    def init_templates(self):
        # split by <!-- HEADER --> and <!-- CONTENT --> from the HTML template into parts
        self.logger.info("init_templates()")
        templates = [
            {
                "template": "pass.html",
                "parts": self.pass_html_template_parts,
            },
            {
                "template": "error.html",
                "parts": self.error_html_template_parts,
            },
            {
                "template": "rejected.html",
                "parts": self.rejected_html_template_parts,
            },
            {
                "template": "fail.html",
                "parts": self.fail_html_template_parts,
            },
        ]
        for template in templates:
            html_template_path = os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    f"assets/email_templates/{template['template']}",
                )
            )

            with open(html_template_path, "r") as f:
                email_template = ""
                for line in f:
                    email_template += line
                    # if line contains <!-- HEADER -->
                    if "<!-- HEADER -->" in line:
                        template["parts"].append(email_template)
                        email_template = ""

                    if "<!-- CONTENT -->" in line:
                        template["parts"].append(email_template)
                        email_template = ""

                template["parts"].append(email_template)

    def get_falcap_web_wo_link(self, work_order):
        return f"<a href='{self._falcap_web_url}?work-order-number={work_order.id}'>{work_order.id}</a>"

    def send_rejected_email(self, work_order):
        self.logger.info("send_rejected_email() for work order " + work_order.id)
        receivers = self._email_config.email_rejected_group
        subject = f"FalCAP blocked: {work_order.id}"
        header_part = self.rejected_html_template_parts[0]
        body_part = self.rejected_html_template_parts[1]
        footer_part = self.rejected_html_template_parts[2]
        email_templates = [
            header_part,
            f"Work Order {self.get_falcap_web_wo_link(work_order)} has been blocked!",
            body_part,
        ]
        if work_order.pens_count < work_order.test_type.min_pen_ct:
            email_templates.append(
                f"""Work Order {self.get_falcap_web_wo_link(work_order)} has been blocked because {work_order.pens_count} 
                pens have been found of the required {work_order.test_type.min_pen_ct}.<br />"""
            )
        else:
            email_templates.append(
                f"""The work order {self.get_falcap_web_wo_link(work_order)} has been blocked. It is possible that the
                work order cannot matched to a type<br />"""
            )

        email_templates.append(
            f"""It has been a minimum of {work_order.test_type.days_to_process_wo_ct} days since 
                data has been entered into the database. This work order will be marked as blocked
                until the test is manually initiated."""
        )
        email_templates.append(footer_part)
        email_template = "".join(email_templates)

        self._send_email(receivers, subject, email_template)

    def send_failed_email(self, work_order, reasons):
        self.logger.info("send_failed_email() for work order " + work_order.id)
        receivers = self._email_config.email_fail_group
        subject = f"FalCAP failed: {work_order.id}"
        header_part = self.fail_html_template_parts[0]
        body_part = self.fail_html_template_parts[1]
        footer_part = self.fail_html_template_parts[2]
        email_templates = [
            header_part,
            f"Work Order {self.get_falcap_web_wo_link(work_order)} has FAILED!",
            body_part,
            "The workorder has failed for these reason(s):<br/>",
        ]
        for reason in reasons:
            email_templates.append(f"{reason} was out of spec.<br/>")
        email_templates.append(footer_part)
        email_template = "".join(email_templates)
        self._send_email(receivers, subject, email_template)

    def send_passed_email(self, work_orders):
        work_order_ids = ""
        for work_order in work_orders:
            if work_order_ids:
                work_order_ids += ", "
            work_order_ids += work_order.id

        self.logger.info(f"send_passed_email() for work orders {work_order_ids}")
        receivers = self._email_config.email_pass_group
        subject = f"FalCAP Pass: {work_order_ids}"
        header_part = self.pass_html_template_parts[0]
        body_part = self.pass_html_template_parts[1]
        footer_part = self.pass_html_template_parts[2]
        email_templates = [
            header_part,
            body_part,
        ]
        for work_order in work_orders:
            email_templates.append(f"{self.get_falcap_web_wo_link(work_order)}<br/>")
        email_templates.append(footer_part)
        email_template = "".join(email_templates)
        self._send_email(receivers, subject, email_template)

    def send_error_email(self, error_buffer):
        self.logger.info(f"send_error_email() for error")
        receivers = self._email_config.email_error_group
        subject = "FalCAP Error"
        header_part = self.error_html_template_parts[0]
        footer_part = self.error_html_template_parts[1]
        email_templates = [
            header_part,
            "<br/>Log:<br/>",
        ]
        for line in error_buffer:
            email_templates.append(f"{line}<br/>")
        email_templates.append(footer_part)

        email_template = "".join(email_templates)
        self._send_email(receivers, subject, email_template)

    def _send_email(self, receivers, subject, email_template):
        try:
            # check if the connection is still alive
            self._server.noop()[0]
            sender_email = self._email_config.email_sender
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = sender_email
            message["To"] = ", ".join(receivers)
            email_template = email_template
            message.attach(MIMEText(email_template, "html"))
            self._server.sendmail(sender_email, receivers, message.as_string())
        except smtplib.SMTPServerDisconnected:
            self.close_connection()
            self.init_smtp()
            self._send_email(receivers, subject, email_template)

    def close_connection(self):
        self._server.quit()
