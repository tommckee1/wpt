#!/usr/bin/python
import json
import urllib
from queue import Empty

from mod_pywebsocket import msgutil
from wptserve import stash

address, authkey = stash.load_env_config()
stash = stash.Stash("msg_channel", address=address, authkey=authkey)


def web_socket_do_extra_handshake(request):
    return


def web_socket_transfer_data(request):
    uuid, direction = parse_request(request)
    print("Got web_socket_transfer_data %s %s" % (uuid, direction))

    with stash.lock:
        value = stash.take(uuid)
        if value is None:
            queue = stash.get_queue()
            if direction == "read":
                has_reader = True
                writer_count = 0
            else:
                has_reader = False
                writer_count = 1
        else:
            queue, has_reader, writer_count = value
            if direction == "read":
                if has_reader:
                    raise ValueError("Tried to start multiple readers for the same queue")
            else:
                writer_count += 1

        stash.put(uuid, (queue, has_reader, writer_count))

    while True:
        if direction == "read":
            try:
                data = queue.get(True, 1)
            except Empty:
                if request.server_terminated:
                    break
            else:
                print("Got data %r" % data)
                msgutil.send_message(request, json.dumps(data))

        elif direction == "write":
            line = request.ws_stream.receive_message()
            if line is None:
                break
            data = json.loads(line)
            print(f"Putting data {line}")
            queue.put(data)

    shutdown(uuid, direction)


def web_socket_passive_closing_handshake(request):
    uuid, direction = parse_request(request)
    print("Got web_socket_passive_closing_handshake %s %s" % (uuid, direction))
    shutdown(uuid, direction)
    return request.ws_close_code, request.ws_close_reason


def parse_request(request):
    query = request.unparsed_uri.split('?')[1]
    GET = dict(urllib.parse.parse_qsl(query))
    uuid = GET["uuid"]
    direction  = GET["direction"]
    return uuid, direction


def shutdown(uuid, direction):
    # Decrease the refcount of the queue
    print("Got shutdown %s %s" % (uuid, direction))
    with stash.lock:
        data = stash.take(uuid)
        if data is None:
            return
        queue, has_reader, writer_count = data
        if direction == "read":
            has_reader = False
        else:
            writer_count -= 1

        if has_reader and writer_count > 0:
            stash.put(uuid, (queue, has_reader, writer_count))
