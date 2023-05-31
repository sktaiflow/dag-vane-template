import pendulum

from airflow.models import Variable
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.sktvane.macros.vault import get_secrets
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

DEFAULT_SLACK_CHANNEL = "#svc_qfeed_engine_noti_dev"


class SktSlackHook(SlackHook):
    """ """

    def __init__(
        self,
        slack_conn_token=None,
        message="",
        attachments=None,
        blocks=None,
        channel=None,
        username=None,
        icon_emoji=None,
        icon_url=None,
        link_names=False,
        proxy=None,
        *args,
        **kwargs,
    ):
        super().__init__(token=slack_conn_token, proxy=proxy, *args, **kwargs)
        self.message = message
        self.attachments = attachments
        self.blocks = blocks
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.icon_url = icon_url
        self.link_names = link_names
        self.proxy = proxy

    def send_block(self):
        return self.client.chat_postMessage(
            channel=self.channel, blocks=self.blocks, username=self.username
        )

    def send_msg(self):
        return self.client.chat_postMessage(
            channel=self.channel, text=self.message, username=self.username
        )


def _get_userID(email):
    token = get_secrets(path="airflow_k8s/adot_slack/slack_alarmbot_token")["token"]
    proxy = get_secrets(path="proxy")["proxy"]

    client = WebClient(token=token, proxy=proxy)
    try:
        response = client.users_lookupByEmail(email=email)
        user = response.get("user")["id"]
        return "<@" + user + ">"
    except SlackApiError:
        return "Wrong email parameter input! Use slack login Email(sk email)"


def _get_userIDs(emails):
    token = get_secrets(path="airflow_k8s/adot_slack/slack_alarmbot_token")["token"]
    proxy = get_secrets(path="proxy")["proxy"]
    client = WebClient(token=token, proxy=proxy)

    userIDs = []
    for email in emails:
        try:
            response = client.users_lookupByEmail(email=email)
            user = response.get("user")["id"]
            userIDs.append("<@" + user + ">")
        except SlackApiError:
            userIDs.append(f"Wrong Email: {email}")
    return ",".join(userIDs)


def _generate_fail_block(context, userID):
    ti = context["ti"]
    logical_date = (
        pendulum.parse(context["ts"]).in_tz("UTC").strftime("%Y-%m-%d %H:%M:%S %Z")
    )

    run_date_key = "data_interval_end"

    run_date = context[run_date_key]
    kst_run = (
        pendulum.timezone("Asia/Seoul")
        .convert(run_date)
        .strftime("%Y-%m-%d %H:%M:%S %Z")
    )
    dag_id = ti.dag_id
    task_id = ti.task_id

    log_url = ti.log_url

    duration = ti.duration
    if duration:
        duration = str(duration) + " second"

    block = [
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": ":red_circle: DAG TASK FAIL ALERT :red_circle:",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Dag*: {dag_id}\n*Task*: {task_id}\n*Logical Date(UTC)*: `{logical_date}`\n"
                f"*Run Date(KST)*: `{kst_run}`\n*Task Duration*: `{duration}`",
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": ":red_circle: Fail Log"},
                "style": "danger",
                "url": log_url,
            },
            "fields": [
                {"type": "mrkdwn", "text": f"*Dag Owner*: {userID}"},
            ],
        },
        {"type": "divider"},
    ]
    return block


def get_fail_alert(channel=DEFAULT_SLACK_CHANNEL, email=""):
    def callback(context):

        proxy = get_secrets(path="proxy")["proxy"]
        env = Variable.get("env")

        username = "Airflow-AlarmBot-prd"
        if env != "prd":
            username = f"Airflow-AlarmBot-{env}"

        userID = "empty email parameter at get_fail_alert method"
        if email and type(email) == str:
            userID = _get_userID(email)
        elif email and type(email) == list:
            userID = _get_userIDs(email)

        block = _generate_fail_block(context, userID)

        slack_hook = SktSlackHook(
            slack_conn_token=get_secrets(path="airflow_k8s/adot_slack/slack_alarmbot_token")[
                "token"
            ],
            proxy=proxy,
            username=username,
            channel=channel,
            blocks=block,
        )

        slack_hook.send_block()

    return callback
