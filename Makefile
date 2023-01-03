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

GOLANG_VERSION := 1.18

ifeq ($(IMAGE),)
REGISTRY ?= nvidia
IMAGE=$(REGISTRY)/operator-libs
endif
IMAGE_TAG ?= $(GOLANG_VERSION)
BUILDIMAGE ?= $(IMAGE):$(IMAGE_TAG)-devel

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.24

CHECK_TARGETS := assert-fmt vet lint ineffassign misspell
TARGETS := binary build all check fmt generate test $(CHECK_TARGETS)
DOCKER_TARGETS := $(patsubst %, docker-%, $(TARGETS))
.PHONY: $(TARGETS) $(DOCKER_TARGETS)

GOOS := linux

TOOLSDIR=$(CURDIR)/bin

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

build:
	GOOS=$(GOOS) go build ./...

all: check build binary
check: $(CHECK_TARGETS)

# Apply go fmt to the codebase
fmt:
	go list -f '{{.Dir}}' $(MODULE)/... \
		| xargs gofmt -s -l -w

assert-fmt:
	go list -f '{{.Dir}}' $(MODULE)/... \
		| xargs gofmt -s -l > fmt.out
	@if [ -s fmt.out ]; then \
		echo "\nERROR: The following files are not formatted:\n"; \
		cat fmt.out; \
		rm fmt.out; \
		exit 1; \
	else \
		rm fmt.out; \
	fi

lint:
	# We use `go list -f '{{.Dir}}' $(MODULE)/...` to skip the `vendor` folder.
	go list -f '{{.Dir}}' $(MODULE)/... | xargs golint -set_exit_status

vet:
	go vet $(MODULE)/...

ineffassign:
	ineffassign $(MODULE)/...

misspell:
	misspell $(MODULE)/...

generate: controller-gen ## Generate code
	$(CONTROLLER_GEN) object object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

test-gen:
	go generate $(MODULE)/...

ENVTEST_ASSETS_DIR=$(shell pwd)/testbin
test: test-gen vet; $(info  running $(NAME:%=% )tests...) @ ## Run tests
	mkdir -p ${ENVTEST_ASSETS_DIR}
	test -f ${ENVTEST_ASSETS_DIR}/setup-envtest.sh || curl -sSLo ${ENVTEST_ASSETS_DIR}/setup-envtest.sh https://raw.githubusercontent.com/kubernetes-sigs/controller-runtime/v0.8.0/hack/setup-envtest.sh
	. ${ENVTEST_ASSETS_DIR}/setup-envtest.sh; fetch_envtest_tools $(ENVTEST_ASSETS_DIR); setup_envtest_env $(ENVTEST_ASSETS_DIR); go test ./... -coverprofile cover.out

CONTROLLER_GEN = $(shell pwd)/bin/controller-gen
controller-gen:	## Download controller-gen locally if necessary
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen@v0.10.0)

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

$(DOCKER_TARGETS): docker-%: .build-image
	@echo "Running 'make $(*)' in docker container $(BUILDIMAGE)"
	$(DOCKER) run \
		--rm \
		-e GOCACHE=/tmp/.cache \
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
