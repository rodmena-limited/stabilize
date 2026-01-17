HTTP Requests
=============

This example shows how to perform HTTP operations, including retries and JSON handling.

.. code-block:: python

    # Stage 1: POST data
    context={
        "url": "https://api.example.com/items",
        "method": "POST",
        "json": {"name": "item1"},
        "retries": 3
    }

See ``examples/http-example.py`` for the full runnable code.
