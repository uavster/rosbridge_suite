# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import uuid

from rclpy.time import Time
# TODO(@jubeira): Re-add once rosauth is ported to ROS2.
# from rosauth.srv import Authentication

import sys
import threading
import traceback
from functools import partial, wraps
import socket
import time

from tornado import version_info as tornado_version_info
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.websocket import WebSocketHandler, WebSocketClosedError
from tornado.gen import coroutine, BadYieldError

from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import json, bson

from std_msgs.msg import Int32


def _log_exception():
    """Log the most recent exception to ROS."""
    exc = traceback.format_exception(*sys.exc_info())
    RosbridgeWebSocket.node_handle.get_logger().error(''.join(exc))


def log_exceptions(f):
    """Decorator for logging exceptions to ROS."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            _log_exception()
            raise
    return wrapper
  

def _get_raw_socket(handler):
    """
    Return the underlying socket.socket for a Tornado WebSocketHandler,
    or None if it can't be located. Works across Tornado versions.
    """
    candidates = []
    # Newer Tornado (>=4.x): HTTP/1 connection holds the IOStream.
    conn = getattr(handler.request, "connection", None)
    if conn is not None:
        candidates.append(getattr(conn, "stream", None))
        # Some versions: connection.detach() returns the stream; some keep `iostream`.
        candidates.append(getattr(conn, "iostream", None))
    # Some Tornado releases exposed it directly on the WS connection.
    ws_conn = getattr(handler, "ws_connection", None)
    if ws_conn is not None:
        candidates.append(getattr(ws_conn, "stream", None))

    for s in candidates:
        sock = getattr(s, "socket", None)
        if sock is not None:
            return sock
    return None
  

class RosbridgeWebSocket(WebSocketHandler):
    client_id_seed = 0
    clients_connected = 0
    authenticate = False
    use_compression = False

    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600                  # seconds
    # protocol.py:
    delay_between_messages = 0              # seconds
    max_message_size = 10000000             # bytes
    unregister_timeout = 10.0               # seconds
    bson_only_mode = False
    node_handle = None


    @log_exceptions
    def open(self):
        cls = self.__class__
        parameters = {
            "fragment_timeout": cls.fragment_timeout,
            "delay_between_messages": cls.delay_between_messages,
            "max_message_size": cls.max_message_size,
            "unregister_timeout": cls.unregister_timeout,
            "bson_only_mode": cls.bson_only_mode
        }
        try:
            self.protocol = RosbridgeProtocol(cls.client_id_seed, cls.node_handle, parameters=parameters)
            self.protocol.outgoing = self.send_message
            self.set_nodelay(True)
            self.authenticated = False
            self._write_lock = threading.RLock()
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.request.remote_ip)
        except Exception as exc:
            cls.node_handle.get_logger().error("Unable to accept incoming connection.  Reason: {}".format(exc))
            # Force-close so we don't leave a phantom handler around.
            try: self.close()
            except Exception: pass
            return

        # Capture the IOLoop that owns this handler so worker threads can
        # schedule writes from any thread safely (Python 3.10 asyncio no longer
        # auto-creates a loop in non-main threads).
        self._ioloop = IOLoop.current()

        sock = _get_raw_socket(self)
        if sock is None:
            cls.node_handle.get_logger().warn("Could not locate underlying socket; TCP keepalive not enabled.")
        else:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                if sys.platform.startswith("linux"):
                    for opt_name, val in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10),
                                      ("TCP_KEEPCNT", 3), ("TCP_USER_TIMEOUT", 60_000)):
                        if hasattr(socket, opt_name):
                            sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt_name), val)
                # Verify
                ka = sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
                cls.node_handle.get_logger().info(
                    "TCP keepalive enabled (SO_KEEPALIVE={}) on fd={}".format(ka, sock.fileno()))
            except Exception as e:
                cls.node_handle.get_logger().warn(
                    "Failed to enable TCP keepalive: {}".format(e))                
        
        cls.node_handle.get_logger().info("Client connected. {} clients total.".format(cls.clients_connected))
        if cls.authenticate:
            cls.node_handle.get_logger().info("Awaiting proper authentication...")

    @log_exceptions
    def on_message(self, message):
        cls = self.__class__
        # check if we need to authenticate
        if cls.authenticate and not self.authenticated:
            try:
                if cls.bson_only_mode:
                    msg = bson.BSON(message).decode()
                else:
                    msg = json.loads(message)

                if msg['op'] == 'auth':
                    # check the authorization information
                    auth_srv_client = cls.node_handle.create_client(Authentication, 'authenticate')
                    auth_srv_req = Authentication.Request()
                    auth_srv_req.mac = msg['mac']
                    auth_srv_req.client = msg['client']
                    auth_srv_req.dest = msg['dest']
                    auth_srv_req.rand = msg['rand']
                    auth_srv_req.t = Time(seconds=msg['t']).to_msg()
                    auth_srv_req.level = msg['level']
                    auth_srv_req.end = Time(seconds=msg['end']).to_msg()

                    while not auth_srv_client.wait_for_service(timeout_sec=1.0):
                        cls.node_handle.get_logger().info('Authenticate service not available, waiting again...')

                    future = auth_srv_client.call_async(auth_srv_req)
                    rclpy.spin_until_future_complete(cls.node_handle, future)

                    # Log error if service could not be called.
                    if future.result() is not None:
                        self.authenticated = future.result().authenticated
                    else:
                        self.authenticated = False
                        cls.node_handle.get_logger.error('Authenticate service call failed')

                    if self.authenticated:
                        cls.node_handle.get_logger().info("Client {} has authenticated.".format(self.protocol.client_id))
                        return
                # if we are here, no valid authentication was given
                cls.node_handle.get_logger().warn(
                    "Client {} did not authenticate. Closing connection.".format(self.protocol.client_id))
                self.close()
            except:
                # proper error will be handled in the protocol class
                self.protocol.incoming(message)
        else:
            # no authentication required
            self.protocol.incoming(message)

    @log_exceptions
    def on_close(self):
        cls = self.__class__
        cls.clients_connected = max(0, cls.clients_connected - 1)
        try:
          if getattr(self, "protocol", None) is not None:
            self.protocol.finish()
        except Exception:
          pass
        if cls.client_manager:
          try:
            cls.client_manager.remove_client(self.client_id, self.request.remote_ip)
          except Exception:
            pass
            
        try:
            if getattr(self, "_watchdog", None) is not None:
                IOLoop.current().remove_timeout(self._watchdog)
                self._watchdog = None
        except Exception:
            pass
        
        cls.node_handle.get_logger().info("Client disconnected. {} clients total.".format(cls.clients_connected))

    def send_message(self, message):
        if type(message) == bson.BSON:
            binary = True
        elif type(message) == bytearray:
            binary = True
            message = bytes(message)
        else:
            binary = False

        # Note: do NOT take self._write_lock here — add_callback is thread-safe
        # and the lock is only needed inside prewrite_message which runs on
        # the IOLoop thread.
        loop = getattr(self, "_ioloop", None)
        if loop is None:
            # Defensive fallback: try to find the running loop (should not happen).
            try:
                loop = IOLoop.current()
            except Exception:
                return  # nothing we can do; connection is shutting down
        loop.add_callback(partial(self.prewrite_message, message, binary))

    @coroutine
    def prewrite_message(self, message, binary):
        cls = self.__class__
        # Use a try block because the log decorator doesn't cooperate with @coroutine.
        try:
            with self._write_lock:
                future = self.write_message(message, binary)

                # When closing, self.write_message() return None even if it's an undocument output.
                # Consider it as WebSocketClosedError
                # For tornado versions <4.3.0 self.write_message() does not have a return value
                if future is None and tornado_version_info >= (4,3,0,0):
                    raise WebSocketClosedError

                yield future
        except WebSocketClosedError:
            cls.node_handle.get_logger().warn('WebSocketClosedError: Tried to write to a closed websocket',
                throttle_duration_sec=1.0)
            raise
        except StreamClosedError:
            cls.node_handle.get_logger().warn('StreamClosedError: Tried to write to a closed stream',
                throttle_duration_sec=1.0)
            raise
        except BadYieldError:
            # Tornado <4.5.0 doesn't like its own yield and raises BadYieldError.
            # This does not affect functionality, so pass silently only in this case.
            if tornado_version_info < (4, 5, 0, 0):
                pass
            else:
                _log_exception()
                raise
        except:
            _log_exception()
            raise

    @log_exceptions
    def check_origin(self, origin):
        return True

    @log_exceptions
    def get_compression_options(self):
        # If this method returns None (the default), compression will be disabled.
        # If it returns a dict (even an empty one), it will be enabled.
        cls = self.__class__

        if not cls.use_compression:
            return None

        return {}
