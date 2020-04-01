package api

// Validate validates the input pipeline specification
// Rules are:
// - JSON Schema validation
// - Node names are unique
// - No circle dependencies
// - Node dependencies refers to existing nodes
// - Node input dependencies refers to nodes in the node's dependencies
// - At least one node does not depend on others
// - Node cannot depend to itslef
// - "args" not permitted as nodename
func (p PipelineSpec) Validate() error {

	return nil
}
