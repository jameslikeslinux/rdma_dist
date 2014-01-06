* Support IPv6 for RDMA CM.  Wouldn't take much...I've just hardcoded AF_INET in several places.
* Add mechanism to set default port options (like num_buffers and buffer_size).
* Explore more advanced RDMA stuff: shared receive queues?, dynamic buffer management?, RDMA reads and writes vs. send and recv?
* Are we doing flow-control the right way?
