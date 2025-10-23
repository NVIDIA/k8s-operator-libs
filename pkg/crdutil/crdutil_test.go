/*
Copyright 2024 NVIDIA CORPORATION & AFFILIATES

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

package crdutil

import (
	"context"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const eventuallyTimeout = 5 * time.Second

var _ = Describe("CRDUtil", func() {
	var (
		ctx            context.Context
		crdsDir        string
		updatedCRDsDir string
		nestedDir      string
	)

	BeforeEach(func() {
		ctx = context.Background()
		crdsDir = "test-files/crds"
		updatedCRDsDir = "test-files/updated-crds"
		nestedDir = "test-files/nested"
	})

	AfterEach(func() {
		testCRDClient.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

		// Wait for cleanup to complete
		Eventually(func() int {
			crds, _ := testCRDClient.List(ctx, metav1.ListOptions{})
			if crds == nil {
				return 0
			}
			return len(crds.Items)
		}, eventuallyTimeout).Should(Equal(0))
	})

	Describe("ProcessCRDsWithConfig", func() {
		It("should create CRDs from a directory", func() {
			// Apply CRDs from the crds directory
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs were created
			crd1, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd1.Name).To(Equal("foos.example.com"))
			Expect(crd1.ResourceVersion).NotTo(BeEmpty())

			crd2, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd2.Name).To(Equal("bars.example.com"))
			Expect(crd2.ResourceVersion).NotTo(BeEmpty())
		})

		It("should update existing CRDs when applying updated versions", func() {
			// First, create the initial CRDs
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Get the initial resource versions
			initialFoo, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			initialFooResourceVersion := initialFoo.ResourceVersion

			initialBar, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			initialBarResourceVersion := initialBar.ResourceVersion

			// Now apply the updated CRDs
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, updatedCRDsDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs were updated by checking resource version changed
			updatedFoo, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedFoo.ResourceVersion).NotTo(Equal(initialFooResourceVersion))

			updatedBar, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedBar.ResourceVersion).NotTo(Equal(initialBarResourceVersion))
		})

		It("should delete CRDs from a directory", func() {
			// Apply CRDs
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Delete CRDs
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs were deleted
			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())
		})

		It("should handle apply and delete operations idempotently", func() {
			// Apply twice
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs exist
			_, err = testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Delete twice
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, crdsDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs are deleted
			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())
		})

		It("should recursively walk directories and apply CRDs from nested subdirectories", func() {
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, nestedDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs were created
			crd1, err := testCRDClient.Get(ctx, "nestedfoos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd1.Name).To(Equal("nestedfoos.example.com"))
			Expect(crd1.ResourceVersion).NotTo(BeEmpty())

			crd2, err := testCRDClient.Get(ctx, "nestedbars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd2.Name).To(Equal("nestedbars.example.com"))
			Expect(crd2.ResourceVersion).NotTo(BeEmpty())

			// Clean up
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, nestedDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify deletion
			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "nestedfoos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "nestedbars.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())
		})

		It("should accept individual file paths and apply CRDs", func() {
			// Apply a single CRD file
			singleFilePath := filepath.Join(crdsDir, "test-crds.yaml")
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, singleFilePath)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs were created
			crd1, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd1.Name).To(Equal("foos.example.com"))

			crd2, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd2.Name).To(Equal("bars.example.com"))

			// Clean up using the same file path
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, singleFilePath)
			Expect(err).NotTo(HaveOccurred())

			// Verify deletion
			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())
		})

		It("should accept multiple directories as variadic arguments", func() {
			// Apply CRDs from multiple directories at once
			err := ProcessCRDsWithConfig(ctx, cfg, CRDOperationApply, crdsDir, nestedDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify CRDs from first directory were created
			crd1, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd1.Name).To(Equal("foos.example.com"))

			crd2, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd2.Name).To(Equal("bars.example.com"))

			// Verify CRDs from nested directory were created
			crd3, err := testCRDClient.Get(ctx, "nestedfoos.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd3.Name).To(Equal("nestedfoos.example.com"))

			crd4, err := testCRDClient.Get(ctx, "nestedbars.example.com", metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(crd4.Name).To(Equal("nestedbars.example.com"))

			// Clean up using the same multiple directories
			err = ProcessCRDsWithConfig(ctx, cfg, CRDOperationDelete, crdsDir, nestedDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify all CRDs were deleted
			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "foos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "bars.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "nestedfoos.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())

			Eventually(func() bool {
				_, err := testCRDClient.Get(ctx, "nestedbars.example.com", metav1.GetOptions{})
				return apierrors.IsNotFound(err)
			}, eventuallyTimeout).Should(BeTrue())
		})
	})
})
