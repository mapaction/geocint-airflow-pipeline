import json
import logging
import os
import sys

# sudo pip3 install slackclient
from slack import WebClient
from slack.errors import SlackApiError

logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_slack_message(token, message):

    client = WebClient(token=token)

    print("sending slack message")

    channel = message['channel']
    username = message['username']
    icon_emoji = message['emoji']
    text = message['text']
    print(f"You entered: {text}")
    blocks = None
    try:
        blocks = json.loads(text)
        text = None
    except json.decoder.JSONDecodeError:
        pass

    try:
        response = client.chat_postMessage(
            icon_emoji=":" + icon_emoji + ":",
            username=username,
            channel=channel,
            text=text,
            blocks=blocks,
            type="mrkdwn",
            unfurl_links=False
        )
        print("Script finished execution")
        logging.info(f"Slack API response: {response}")
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        logging.error(f"Slack API error response: {e.response}")
        print("Script finished execution 2")
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'

