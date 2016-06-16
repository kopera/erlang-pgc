.PHONY:: all build
all: build analyze

build: rebar3
	@./rebar3 compile

# Development
.PHONY:: develop analyze
develop: rebar3
	@echo "Starting development mode"
	@echo ""
	@echo "Once you get to the Erlang shell, you can try the pgsql driver with:"
	@echo "    application:ensure_all_started(pgsql)."
	@echo "    pgsql_demo:main(\"select 1 + 1 as sum\", [])."
	@echo ""
	@echo ""
	@./rebar3 auto

analyze: rebar3
	@./rebar3 dialyzer

# Clean
.PHONY:: clean distclean
clean: rebar3
	@./rebar3 clean --all

distclean: clean
	$(RM) -R _build/

# Internal
REBAR3_URL = https://s3.amazonaws.com/rebar3/rebar3
rebar3:
	@echo "\033[0;36m===> Downloading rebar\033[0m"
	@wget --quiet -O "$@" $(REBAR3_URL) || rm "$@"
	@chmod +x "$@"
