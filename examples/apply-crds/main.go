/*
Copyright 2025 NVIDIA CORPORATION & AFFILIATES

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package main demonstrates how to use the crdutil package to manage CRDs.
// This is a simple example that shows how to apply and delete CRDs from directories or files.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/NVIDIA/k8s-operator-libs/pkg/crdutil"
)

var (
	crdsPath  string
	operation string
)

func init() {
	flag.StringVar(&crdsPath, "crds-path", "",
		"Path to CRD manifest file or directory (directories are searched recursively)")
	flag.StringVar(&operation, "operation", "apply", "Operation to perform: 'apply' or 'delete'")
}

func main() {
	flag.Parse()

	if crdsPath == "" {
		log.Fatal("Error: --crds-path flag is required")
	}

	ctx := context.Background()

	switch operation {
	case "apply":
		if err := crdutil.ProcessCRDs(ctx, crdutil.CRDOperationApply, crdsPath); err != nil {
			log.Fatalf("Failed to apply CRDs: %v", err)
		}
	case "delete":
		if err := crdutil.ProcessCRDs(ctx, crdutil.CRDOperationDelete, crdsPath); err != nil {
			log.Fatalf("Failed to delete CRDs: %v", err)
		}
	default:
		log.Fatalf("Invalid operation: %s (must be 'apply' or 'delete')", operation)
	}
}
