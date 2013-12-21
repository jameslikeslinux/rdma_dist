default: compile

compile:
	./rebar compile

run: compile
	ERL_LIBS=deps:$(shell readlink -f ..) erl +K true -pa ebin -proto_dist rdma

test: compile
	./rebar -v skip_deps=true ct

clean:
	-./rebar clean
	-rm -rf test/*.beam
