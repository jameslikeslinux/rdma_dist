default: compile

compile:
	./rebar compile

run: compile
	erl +K true -pa ebin -name test@login-1-ib.deepthought.umd.edu -proto_dist rdma

test: compile
	./rebar -v skip_deps=true ct

clean:
	-./rebar clean
	-rm -rf test/*.beam
