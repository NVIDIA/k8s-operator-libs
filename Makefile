# Copyright (c) 2021, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MODULE := github.com/NVIDIA/k8s-operator-libs

DOCKER ?= docker

GOLANG_VERSION := 1.24

ifeq ($(IMAGE),)
REGISTRY ?= nvidia
IMAGE=$(REGISTRY)/operator-libs
endif
IMAGE_TAG ?= $(GOLANG_VERSION)
BUILDIMAGE ?= $(IMAGE):$(IMAGE_TAG)-devel

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.32.x

TARGETS := all check lint go-check generate test cov-report controller-gen golangci-lint gcov2lcov
DOCKER_TARGETS := $(patsubst %, docker-%, $(TARGETS))
.PHONY: $(TARGETS) $(DOCKER_TARGETS)

GOOS := linux

# Tools
TOOLSDIR=$(CURDIR)/bin

GOLANGCILINT ?= $(TOOLSDIR)/golangci-lint
CONTROLLER_GEN ?= $(TOOLSDIR)/controller-gen
GCOV2LCOV ?= $(TOOLSDIR)/gcov2lcov
SETUP_ENVTEST ?= $(TOOLSDIR)/setup-envtest
GOLANGCILINT_VERSION ?= v2.0.2
CONTROLLER_GEN_VERSION ?= v0.16.5
GCOV2LCOV_VERSION ?= v1.1.1
SETUP_ENVTEST_RELEASE ?= release-0.19

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

all: generate check test cov-report ## Generate code, run checks and tests

check: lint go-check ## Run linters and go checks

lint: golangci-lint ## Lint code
	$(GOLANGCILINT) run --timeout 10m

go-check: ## Run go checks to ensure modules are synced
	go mod tidy && git diff --exit-code

generate: controller-gen ## Generate code
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."
	go generate $(MODULE)/...

test: setup-envtest generate; $(info  running $(NAME:%=% )tests...) @ ## Run tests
	export KUBEBUILDER_ASSETS="$(shell $(SETUP_ENVTEST) use -p path $(ENVTEST_K8S_VERSION))" && \
	go test ./... -coverprofile cover.out

cov-report: gcov2lcov test ## Build test coverage report in lcov format
	$(GCOV2LCOV) -infile cover.out -outfile lcov.info

controller-gen:	## Download controller-gen locally if necessary
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION))

golangci-lint: ## Download golangci-lint locally if necessary.
	$(call go-install-tool,$(GOLANGCILINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCILINT_VERSION))

gcov2lcov: ## Download gcov2lcov locally if necessary.
	$(call go-install-tool,$(GCOV2LCOV),github.com/jandelgado/gcov2lcov@$(GCOV2LCOV_VERSION))

setup-envtest: ## Download setup-envtest locally if necessary
	$(call go-install-tool,$(SETUP_ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_RELEASE))

# Generate an image for containerized builds
# Note: This image is local only
.PHONY: .build-image .pull-build-image .push-build-image
.build-image: docker/Dockerfile.devel
	if [ "$(SKIP_IMAGE_BUILD)" = "" ]; then \
		$(DOCKER) build \
			--progress=plain \
			--build-arg GOLANG_VERSION="$(GOLANG_VERSION)" \
			--tag $(BUILDIMAGE) \
			-f $(^) \
			docker; \
	fi

.pull-build-image:
	$(DOCKER) pull $(BUILDIMAGE)

.push-build-image:
	$(DOCKER) push $(BUILDIMAGE)

$(DOCKER_TARGETS): docker-%: .build-image ## Run command in docker
	@echo "Running 'make $(*)' in docker container $(BUILDIMAGE)"
	$(DOCKER) run \
		--rm \
		-e GOCACHE=/tmp/.cache \
		-e GOLANGCI_LINT_CACHE=/tmp/.cache \
		-e PROJECT_DIR=$(PWD) \
		-v $(PWD):$(PWD) \
		-w $(PWD) \
		--user $$(id -u):$$(id -g) \
		$(BUILDIMAGE) \
			make $(*)

# go-install-tool will 'go install' any package $2 and install it to $1.
define go-install-tool
@[ -f $(1) ] || { \
echo "Downloading $(2)" ;\
GOBIN=$(TOOLSDIR) go install $(2) ;\
}
endef

.PHONY: help
help: ## Show this message
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
