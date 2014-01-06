# RDMA Distribution Driver for Erlang #
This is an alternative distribution driver for the Erlang virtual machine.  It
implements a native protocol for RDMA devices such as InfiniBand.  This can
potentially improve messaging latency and throughput compared to the default
TCP distribution protocol.

## Building ##
This software can be added as a dependency in your project using Rebar or Mix.
It requires OFED to be installed.  If it is installed somewhere outside of the
default locations, set the `CFLAGS` and `LDFLAGS` environmental variables
appropriately before compiling.

## Using ##
The software's `ebin` directory must be added to the Erlang VM's code path.
Then you can tell the VM to use the driver by passing the `-proto_dist rdma`
option.  Here is an example:

*   Erlang:

        % erl +K true -pa rdma_dist/ebin -proto_dist rdma -name foo@the-rdma-interface.exmaple.com

*   Elixir:

        % iex --erl "+K true -pa rdma_dist/ebin -proto_dist rdma -name foo@the-rdma-interface.exmaple.com"

Kernel polling (`+K true`) is recommended.  If the RDMA device isn't the
default interface on the system, explicitly name it in the `-name` option.  For
InfiniBand devices, you must have an IP-over-IB device set up---it will be used
for connection management.  Once the connection is established, the driver will
use native RDMA verbs.

## Testing ##
Tests can be run with `make test`.

This software has seen limited testing over the following devices:

*   Qlogic QDR InfiniBand
*   Mellanox QDR InfiniBand

No real-world applications have been tested using this distribution driver.  If
you have success with other RDMA devices or large Erlang applications, let me
know :)
