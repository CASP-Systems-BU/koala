package dataflow_test

import (
	"testing"
)

// Operator Setup() method has nested hierarchy and can be overridden by outer
// implementations. The most-outer implementation will be used than embeded.
// Specifically, there are 2 cases:
// 1. Stateless Operator: Setup() implementation in specific operator is
// optional. If not implemented, the default OperatorBase Setup() method will
// be called.
// 2. Stateful Operator: Setup() implementation in specific operator
// optional. If not implemented, the default StatefulOperatorBase Setup() will
// be called.
// If specific operator needs to override Setup() for specific setup steps, it
// has to call embeded.Setup() to include common setup steps.
// The following test simulates the setup hierarchy.
func TestSetupHierarchy(t *testing.T) {

	var api Api

	api = &CustomNoSetup{}
	if api.Setup() != "StatefulBase" {
		t.Fatalf("Expected StatefulBase, got %s", api.Setup())
	}

	api = &CustomWithSetup{}
	if api.Setup() != "CustomWithSetup" {
		t.Fatalf("Expected CustomWithSetup, got %s", api.Setup())
	}

	api = &CustomWithSetup2{}
	if api.Setup() != "CustomWithSetup2" {
		t.Fatalf("Expected CustomWithSetup2, got %s", api.Setup())
	}
}

type Api interface {
	Setup() string
}

type Base struct {
}

func (b *Base) Setup() string {
	return "Base"
}

type StatefulBase struct {
	Base
}

func (s *StatefulBase) Setup() string {
	s.Base.Setup() // call base setup to execute common setup steps
	return "StatefulBase"
}

type CustomNoSetup struct {
	StatefulBase
}

type CustomWithSetup struct {
	StatefulBase
}

func (c *CustomWithSetup) Setup() string {
	c.StatefulBase.Setup() // call base setup to execute common setup steps
	return "CustomWithSetup"
}

type CustomWithSetup2 struct {
	Base
}

func (c *CustomWithSetup2) Setup() string {
	c.Base.Setup() // call base setup to execute common setup steps
	return "CustomWithSetup2"
}
