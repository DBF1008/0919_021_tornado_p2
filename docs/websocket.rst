``tornado.websocket`` --- Bidirectional communication to the browser
====================================================================

.. testsetup::

   import tornado

.. automodule:: tornado.websocket

   .. autoclass:: WebSocketHandler

   Event handlers
   --------------

   .. automethod:: WebSocketHandler.open
   .. automethod:: WebSocketHandler.on_message
   .. automethod:: WebSocketHandler.on_close
   .. automethod:: WebSocketHandler.select_subprotocol
   .. autoattribute:: WebSocketHandler.selected_subprotocol
   .. automethod:: WebSocketHandler.on_ping

   Output
   ------

   .. automethod:: WebSocketHandler.write_message
   .. automethod:: WebSocketHandler.close

   Configuration
   -------------

   .. automethod:: WebSocketHandler.check_origin
   .. automethod:: WebSocketHandler.get_compression_options
   .. automethod:: WebSocketHandler.set_nodelay
   .. autoattribute:: WebSocketHandler.rate_limit
   .. autoattribute:: WebSocketHandler.rate_limit_window

   Other
   -----

   .. automethod:: WebSocketHandler.ping
   .. automethod:: WebSocketHandler.on_pong
   .. autoexception:: WebSocketClosedError


   Client-side support
   -------------------

   .. autofunction:: websocket_connect
   .. autoclass:: WebSocketClientConnection
       :members:
