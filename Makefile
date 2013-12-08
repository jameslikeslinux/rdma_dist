default: compile

compile:
	./rebar compile

run: compile
	ERL_LIBS=$(shell readlink -f ..) erl +K true

test: compile
	./rebar -v skip_deps=true ct

clean:
	-./rebar clean
	-rm -rf test/*.beam
