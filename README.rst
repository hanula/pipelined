Simple pipeline processing
==========================



Overview
--------
...

Usage examples
--------------

.. code-block:: python

    import pipelined

    def detect_ddos(stream):
        access = pipelined.utils.limit_counter(10*1000)
        for log in stream:
            access[log.ip] += 1
            yield from access.over(100)

    def block_ips(stream):
        for ip in stream:
            # block the ip
            yield ip

    ddos_monitor = [
        detect_ddos,
        block_ips,
    ]

    source_logs = pipelined.files.follow('/path/to/server/access.log')
    for ip in pipelined.run(ddos_monitor, source_logs):
        print(ip, "has been blocked.")

Resources
----------

- Source: https://github.com/hanula/pipelined
- Documentation: https://github.com/hanula/pipelined

License
-------

See `LICENSE <https://github.com/hanula/pipelined/blob/master/LICENSE>`_.
