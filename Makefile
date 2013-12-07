default: compile

compile:
	./rebar compile

run: compile
	ERL_LIBS=$(shell readlink -f ..) erl +K true

clean:
	./rebar clean
