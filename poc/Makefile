ENGINE ?= $(shell command -v podman || command -v docker)
IMAGE ?= recon-sidecar:dev
GOBIN ?= $(shell go env GOPATH)/bin

PROTO_DIR := ../proto
PROTO_FILE := recon/v1/reconciler.proto
PROTO_MAP := M$(PROTO_FILE)=poc/gen/recon/v1;reconv1

# Rootless podman needs --userns=keep-id so that files the container writes
# to a bind mount belong to the host user.
ROOTLESS = $(shell $(ENGINE) info --format '{{.Host.Security.Rootless}}' 2>/dev/null)
USERNS = $(if $(filter true,$(ROOTLESS)),--userns=keep-id)

E2E_ENV := RECON_E2E_IMAGE=$(IMAGE) RECON_E2E_ENGINE=$(ENGINE)
E2E_FLAGS := -tags e2e -count=1 -timeout 45m

RUN_MEMORY ?= 1g

# make reference installs its pinned generators in TOOLS and puts it first
# on PATH, so an older avroc elsewhere can't change the code it generates.
TOOLS := $(CURDIR)/.local/tools
REFERENCE := e2e/gen/reference

# Where protoc finds google/protobuf/timestamp.proto. A protoc built from
# source needs its src directory; a packaged one finds the file itself, so
# a directory that doesn't exist is left out.
PROTOBUF_INCLUDE ?= $(HOME)/github.com/protocolbuffers/protobuf/src

.PHONY: proto test image preflight e2e e2e-short run reference

proto:
	PATH="$(GOBIN):$$PATH" protoc -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=poc --go_opt='$(PROTO_MAP)' \
		--go-grpc_out=. --go-grpc_opt=module=poc --go-grpc_opt='$(PROTO_MAP)' \
		$(PROTO_DIR)/$(PROTO_FILE)

test:
	go test -race ./...

# Re-emit the reference schemas' .proto and .avdl files, then regenerate
# the Go code the byte-exact tests in REFERENCE check values against. The
# emitter doesn't import that code, so this works even when it is broken.
# protoc writes to .local first, so a failed run leaves the old code in
# place.
reference:
	GOBIN=$(TOOLS) go install github.com/z5labs/avroc/cmd/avroc@v0.3.0 github.com/z5labs/avroc/cmd/avroc-gen-go@v0.3.0
	GOBIN=$(TOOLS) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	go run ./$(REFERENCE)/cmd/emit -dir $(REFERENCE)
	rm -rf .local/pb && mkdir -p .local/pb
	protoc -I . $(addprefix -I ,$(wildcard $(PROTOBUF_INCLUDE))) \
		--plugin=protoc-gen-go=$(TOOLS)/protoc-gen-go \
		--go_out=.local/pb --go_opt=module=poc/$(REFERENCE)/pb \
		$(REFERENCE)/schema/*.proto
	rm -rf $(REFERENCE)/pb && mv .local/pb $(REFERENCE)/pb
	cd $(REFERENCE) && PATH="$(TOOLS):$$PATH" avroc generate

image:
	$(ENGINE) build -f Containerfile -t $(IMAGE) .

# The memory tests mean nothing if the engine can't enforce --memory.
preflight:
	@test -n "$(ENGINE)" || { echo "preflight: neither podman nor docker found" >&2; exit 1; }
	@if [ "$$(basename "$$(readlink -f "$(ENGINE)")")" = podman ] || $(ENGINE) --version 2>/dev/null | grep -qi podman; then \
		controllers="$$($(ENGINE) info --format '{{.Host.CgroupControllers}}')"; \
		case "$$controllers" in \
			*memory*) ;; \
			*) echo "preflight: podman's cgroup controllers ($$controllers) lack memory; delegate it to the user session" >&2; exit 1 ;; \
		esac; \
	fi

e2e: preflight image
	$(E2E_ENV) go test $(E2E_FLAGS) ./e2e/...

e2e-short: preflight image
	$(E2E_ENV) go test $(E2E_FLAGS) -short ./e2e/...

run: image
	go run ./e2e/harness/cmd/gencert -dir .local/tls
	mkdir -p .local/data
	$(ENGINE) run --rm $(USERNS) --user "$$(id -u):$$(id -g)" --memory=$(RUN_MEMORY) \
		-p 127.0.0.1:7443:7443 -p 127.0.0.1:8081:8081 \
		-v "$(CURDIR)/.local/tls:/etc/recon/tls:ro,z" \
		-v "$(CURDIR)/.local/data:/var/lib/recon:Z" \
		-e RECON_TLS_CERT_FILE=/etc/recon/tls/server.pem \
		-e RECON_TLS_KEY_FILE=/etc/recon/tls/server-key.pem \
		$(IMAGE)
