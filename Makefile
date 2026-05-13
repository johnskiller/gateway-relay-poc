# ============================================================
# Makefile for zenoh-gateway-poc
# ============================================================

# ---- Configurable Variables ----
REGISTRY    ?= ghcr.io
ORG         ?= your-org
IMAGE_NAME  ?= zenoh-gateway-poc
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
TAG         ?= $(VERSION)

GATEWAY_IMAGE  = $(REGISTRY)/$(ORG)/$(IMAGE_NAME)-gateway:$(TAG)
PRODUCER_IMAGE = $(REGISTRY)/$(ORG)/$(IMAGE_NAME)-producer:$(TAG)
CONSUMER_IMAGE = $(REGISTRY)/$(ORG)/$(IMAGE_NAME)-consumer:$(TAG)

# Rust build target (set to musl for fully static binaries)
RUST_TARGET ?=
CARGO_FLAGS := --release
ifdef RUST_TARGET
CARGO_FLAGS += --target $(RUST_TARGET)
endif

# ---- Help ----
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---- Build ----
.PHONY: build
build: ## Build all binaries in release mode
	cargo build $(CARGO_FLAGS)

.PHONY: build-dev
build-dev: ## Build all binaries in debug mode
	cargo build

.PHONY: check
check: ## Quick check for compilation errors
	cargo check

.PHONY: test
test: ## Run unit tests
	cargo test

# ---- Docker Images ----
.PHONY: image-gateway
image-gateway: ## Build gateway Docker image
	docker build --target gateway -t $(GATEWAY_IMAGE) .

.PHONY: image-producer
image-producer: ## Build producer Docker image
	docker build --target producer -t $(PRODUCER_IMAGE) .

.PHONY: image-consumer
image-consumer: ## Build consumer Docker image
	docker build --target consumer -t $(CONSUMER_IMAGE) .

.PHONY: images
images: image-gateway image-producer image-consumer ## Build all Docker images

# ---- Push ----
.PHONY: push-gateway
push-gateway: ## Push gateway image to registry
	docker push $(GATEWAY_IMAGE)

.PHONY: push-producer
push-producer: ## Push producer image to registry
	docker push $(PRODUCER_IMAGE)

.PHONY: push-consumer
push-consumer: ## Push consumer image to registry
	docker push $(CONSUMER_IMAGE)

.PHONY: push
push: push-gateway push-producer push-consumer ## Push all images to registry

# ---- Clean ----
.PHONY: clean
clean: ## Remove build artifacts
	cargo clean

.PHONY: image-clean
image-clean: ## Remove Docker images
	-docker rmi $(GATEWAY_IMAGE) 2>/dev/null
	-docker rmi $(PRODUCER_IMAGE) 2>/dev/null
	-docker rmi $(CONSUMER_IMAGE) 2>/dev/null

# ---- Info ----
.PHONY: info
info: ## Print build configuration
	@echo "REGISTRY:       $(REGISTRY)"
	@echo "ORG:            $(ORG)"
	@echo "IMAGE_NAME:     $(IMAGE_NAME)"
	@echo "VERSION:        $(VERSION)"
	@echo "TAG:            $(TAG)"
	@echo "GATEWAY_IMAGE:  $(GATEWAY_IMAGE)"
	@echo "PRODUCER_IMAGE: $(PRODUCER_IMAGE)"
	@echo "CONSUMER_IMAGE: $(CONSUMER_IMAGE)"
	@echo "RUST_TARGET:    $(RUST_TARGET)"
