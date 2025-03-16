# src/mqtt_rr/request_response.py
import json
import time
import uuid
import threading
from queue import Queue, Empty

import paho.mqtt.client as mqtt


class MqttRequestResponse:
    """
    A universal MQTT class that can:

      - Send requests (optionally to a custom topic, with a custom reply topic).
      - Handle multiple responses for each request (including multiple servers).
      - Receive requests and send multiple updates back.
      - Invoke a callback when the status of an outgoing request changes (optional).
      - Subscribe to additional topics upon connecting (optional).
      - Include a client_id in messages (as "sender").
      - Support MQTT username and password for broker auth.

    If multiple servers reply to the same request_id, this class will queue all
    responses in the same request�s queue. The application can consume them via
    'poll_next_response(request_id)' or track status changes with a callback.
    """

    def __init__(
        self,
        broker_host="localhost",
        broker_port=1883,
        request_topic="myapp/requests",
        response_topic="myapp/responses",
        timeout_interval=1.0,
        on_request_callback=None,
        on_request_status_change=None,
        on_message=None,
        additional_subscriptions=None,
        client_id=None,
        username=None,
        password=None,
    ):
        """
        :param broker_host: IP/hostname of the MQTT broker
        :param broker_port: Port of the MQTT broker
        :param request_topic: Default topic for sending requests
        :param response_topic: Default topic for sending responses
        :param timeout_interval: How often (seconds) to check for expired requests
        :param on_request_callback:
            Function to handle inbound requests: on_request_callback(request_id, data, reply_fn)
        :param on_request_status_change:
            Function called when an outgoing request changes status:
                on_request_status_change(request_id, old_status, new_status, response_payload)
        :param additional_subscriptions:
            Optional list of additional MQTT topics to subscribe to upon connect.
            Each entry can be either a string (topic) or a (topic, qos) tuple.
        :param client_id:
            An identifier for this instance. If provided, each request/response
            will carry "sender": client_id.
        :param username:
            MQTT username (if broker requires authentication).
        :param password:
            MQTT password (if broker requires authentication).
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.request_topic = request_topic
        self.response_topic = response_topic
        self.timeout_interval = timeout_interval

        self.on_request_callback = on_request_callback or self._default_on_request
        self.on_request_status_change = on_request_status_change
        self.on_message = on_message
        self.additional_subscriptions = additional_subscriptions or []
        self.client_id = client_id

        self.username = username
        self.password = password
        self.innerstatus = {}

        # Track outgoing requests: request_id -> {timestamp, timeout, queue, last_status}
        self.pending_requests = {}

        # Create MQTT client
        self.client = mqtt.Client()

        # If username/password provided, set them
        if self.username is not None and self.password is not None:
            self.client.username_pw_set(self.username, self.password)

        # Setup callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        self.last_will_topic = f"devices/{self.client_id}/status"
        self.client.will_set(
            self.last_will_topic,
            payload=json.dumps({"status": "offline"}),
            retain=True
        )
        # Housekeeping thread (for timeouts)
        self._stop_event = threading.Event()
        self.housekeeping_thread = threading.Thread(
            target=self._housekeeping_loop, daemon=True
        )

    def _on_connect(self, client, userdata, flags, rc):
        print(f"[MQTT] Connected with result code {rc}")
        # Publish "online" status (retained)
        self.innerstatus['status']="online"
        client.publish(
            self.last_will_topic,
            json.dumps(self.innerstatus),
            retain=True
        )
        # Subscribe to default request_topic and response_topic
        client.subscribe(self.request_topic)
        client.subscribe(self.response_topic)

        # Also subscribe to any additional topics
        for topic_info in self.additional_subscriptions:
            if isinstance(topic_info, tuple):
                topic, qos = topic_info
                client.subscribe(topic, qos)
            else:
                client.subscribe(topic_info)

    def _on_message(self, client, userdata, msg):
        """Handle inbound MQTT messages."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception as e:
            print(f"[ERROR] Could not parse JSON from {msg.topic}: {e}")
            return

        msg_type = payload.get("type")         # "request" or "response"
        request_id = payload.get("request_id") # correlation ID
        if not msg_type or not request_id:
            if self.on_message:
               self.on_message(client, msg.topic, payload)
            return  # Possibly irrelevant message

        if msg_type == "request":
            self._handle_inbound_request(payload)
        elif msg_type == "response":
            self._handle_inbound_response(payload)

    def _handle_inbound_request(self, payload):
        """
        Called when a request arrives. We'll call on_request_callback,
        giving the user a reply_fn to send responses.
        """
        request_id = payload["request_id"]
        data = payload.get("data", {})
        reply_topic = payload.get("reply_topic", self.response_topic)

        def reply_fn(status, extra_fields=None, override_topic=None):
            """Send a response back to the requester."""
            if extra_fields is None:
                extra_fields = {}

            response_payload = {
                "type": "response",
                "request_id": request_id,
                "timestamp": time.time(),
                "status": status
            }
            if self.client_id:
                response_payload["sender"] = self.client_id

            response_payload.update(extra_fields)

            topic_to_use = override_topic or reply_topic
            self.client.publish(topic_to_use, json.dumps(response_payload))

        # Dispatch to the user's callback
        self.on_request_callback(request_id, data, reply_fn)

    def _handle_inbound_response(self, payload):
        """Called when a response arrives for one of our outgoing requests."""
        request_id = payload["request_id"]
        if request_id not in self.pending_requests:
            return

        req_info = self.pending_requests[request_id]
        req_info["queue"].put(payload)

        new_status = payload.get("status")
        old_status = req_info["last_status"]
        if new_status and new_status != old_status:
            if self.on_request_status_change:
                self.on_request_status_change(
                    request_id, old_status, new_status, payload
                )
            req_info["last_status"] = new_status

    def _default_on_request(self, request_id, data, reply_fn):
        """
        Default handler if none is provided. Echos data with 'done'.
        """
        print(f"[INFO] Default on_request handler for {request_id}, data={data}")
        reply_fn("received")
        time.sleep(1)
        reply_fn("done", {"echo": data})

    def _housekeeping_loop(self):
        """Periodically check for request timeouts."""
        while not self._stop_event.is_set():
            now = time.time()
            expired = []
            for req_id, info in list(self.pending_requests.items()):
                if now - info["timestamp"] > info["timeout"]:
                    expired.append(req_id)

            for req_id in expired:
                info = self.pending_requests[req_id]
                info["queue"].put({"error": "timeout"})
                if self.on_request_status_change:
                    old_status = info["last_status"]
                    self.on_request_status_change(
                        req_id, old_status, "timeout", {"error": "timeout"}
                    )
                del self.pending_requests[req_id]

            time.sleep(1.0)

    # Public methods

    def connect(self):
        """Connect to the MQTT broker and start threads."""
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()
        self.housekeeping_thread.start()

    def disconnect(self):
        """Stop housekeeping and the MQTT loop, then disconnect."""
        self._stop_event.set()
        self.housekeeping_thread.join(timeout=2)
        self.client.loop_stop()
        self.client.disconnect()

    def send_request(self, data, timeout=10.0, dest_topic=None, reply_topic=None):
        """
        Send a request with a unique request_id, returning the request_id.

        :param data: The dict payload for the request.
        :param timeout: Seconds to track this request before it expires.
        :param dest_topic: Topic to publish the request to.
        :param reply_topic: Topic we expect the server to respond on.
        :return: The generated request_id (str).
        """
        if not dest_topic:
            dest_topic = self.request_topic

        request_id = str(uuid.uuid4())
        payload = {
            "type": "request",
            "request_id": request_id,
            "timestamp": time.time(),
            "data": data
        }
        if self.client_id:
            payload["sender"] = self.client_id
        if reply_topic:
            payload["reply_topic"] = reply_topic

        resp_queue = Queue()
        self.pending_requests[request_id] = {
            "timestamp": time.time(),
            "timeout": timeout,
            "queue": resp_queue,
            "last_status": None,
        }

        self.client.publish(dest_topic, json.dumps(payload))
        return request_id

    def update_status(self, status:dict):
        self.innerstatus = status
        self.innerstatus['status']="online"
        self.client.publish(
            self.last_will_topic,            
            json.dumps(self.innerstatus),
            retain=True
        )


    def poll_next_response(self, request_id, timeout=5.0):
        """
        Get the next response from the queue for this request_id,
        blocking up to `timeout` seconds.
        Returns the response dict or None if none arrives / request is unknown.
        """
        req_info = self.pending_requests.get(request_id)
        if not req_info:
            return None
        try:
            return req_info["queue"].get(timeout=timeout)
        except Empty:
            return None

    def get_response_queue(self, request_id):
        """Return the queue for a given request_id, or None if not found."""
        req_info = self.pending_requests.get(request_id)
        return req_info["queue"] if req_info else None

    def get_latest_status(self, request_id):
        """Return the last known status for a request, or None if unknown."""
        req_info = self.pending_requests.get(request_id)
        if not req_info:
            return None
        return req_info["last_status"]
