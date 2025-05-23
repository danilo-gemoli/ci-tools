// Code generated by smithy-go-codegen DO NOT EDIT.

package types

import (
	"fmt"
	smithy "github.com/aws/smithy-go"
)

// The resource with the name requested already exists.
type AlreadyExistsException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *AlreadyExistsException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *AlreadyExistsException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *AlreadyExistsException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "AlreadyExistsException"
	}
	return *e.ErrorCodeOverride
}
func (e *AlreadyExistsException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// An error occurred during a CloudFormation registry operation.
type CFNRegistryException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *CFNRegistryException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *CFNRegistryException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *CFNRegistryException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "CFNRegistryException"
	}
	return *e.ErrorCodeOverride
}
func (e *CFNRegistryException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified change set name or ID doesn't exit. To view valid change sets for
// a stack, use the ListChangeSets operation.
type ChangeSetNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *ChangeSetNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *ChangeSetNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *ChangeSetNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ChangeSetNotFound"
	}
	return *e.ErrorCodeOverride
}
func (e *ChangeSetNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// No more than 5 generated templates can be in an InProgress or Pending status at
// one time. This error is also returned if a generated template that is in an
// InProgress or Pending status is attempted to be updated or deleted.
type ConcurrentResourcesLimitExceededException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *ConcurrentResourcesLimitExceededException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *ConcurrentResourcesLimitExceededException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *ConcurrentResourcesLimitExceededException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ConcurrentResourcesLimitExceeded"
	}
	return *e.ErrorCodeOverride
}
func (e *ConcurrentResourcesLimitExceededException) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

// The specified resource exists, but has been changed.
type CreatedButModifiedException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *CreatedButModifiedException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *CreatedButModifiedException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *CreatedButModifiedException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "CreatedButModifiedException"
	}
	return *e.ErrorCodeOverride
}
func (e *CreatedButModifiedException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The generated template was not found.
type GeneratedTemplateNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *GeneratedTemplateNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *GeneratedTemplateNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *GeneratedTemplateNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "GeneratedTemplateNotFound"
	}
	return *e.ErrorCodeOverride
}
func (e *GeneratedTemplateNotFoundException) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

// The specified target doesn't have any requested Hook invocations.
type HookResultNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *HookResultNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *HookResultNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *HookResultNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "HookResultNotFound"
	}
	return *e.ErrorCodeOverride
}
func (e *HookResultNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The template contains resources with capabilities that weren't specified in the
// Capabilities parameter.
type InsufficientCapabilitiesException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *InsufficientCapabilitiesException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *InsufficientCapabilitiesException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *InsufficientCapabilitiesException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "InsufficientCapabilitiesException"
	}
	return *e.ErrorCodeOverride
}
func (e *InsufficientCapabilitiesException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified change set can't be used to update the stack. For example, the
// change set status might be CREATE_IN_PROGRESS , or the stack status might be
// UPDATE_IN_PROGRESS .
type InvalidChangeSetStatusException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *InvalidChangeSetStatusException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *InvalidChangeSetStatusException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *InvalidChangeSetStatusException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "InvalidChangeSetStatus"
	}
	return *e.ErrorCodeOverride
}
func (e *InvalidChangeSetStatusException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified operation isn't valid.
type InvalidOperationException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *InvalidOperationException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *InvalidOperationException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *InvalidOperationException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "InvalidOperationException"
	}
	return *e.ErrorCodeOverride
}
func (e *InvalidOperationException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// Error reserved for use by the [CloudFormation CLI]. CloudFormation doesn't return this error to
// users.
//
// [CloudFormation CLI]: https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/what-is-cloudformation-cli.html
type InvalidStateTransitionException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *InvalidStateTransitionException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *InvalidStateTransitionException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *InvalidStateTransitionException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "InvalidStateTransition"
	}
	return *e.ErrorCodeOverride
}
func (e *InvalidStateTransitionException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The quota for the resource has already been reached.
//
// For information about resource and stack limitations, see [CloudFormation quotas] in the
// CloudFormation User Guide.
//
// [CloudFormation quotas]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cloudformation-limits.html
type LimitExceededException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *LimitExceededException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *LimitExceededException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *LimitExceededException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "LimitExceededException"
	}
	return *e.ErrorCodeOverride
}
func (e *LimitExceededException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified name is already in use.
type NameAlreadyExistsException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *NameAlreadyExistsException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *NameAlreadyExistsException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *NameAlreadyExistsException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "NameAlreadyExistsException"
	}
	return *e.ErrorCodeOverride
}
func (e *NameAlreadyExistsException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified operation ID already exists.
type OperationIdAlreadyExistsException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *OperationIdAlreadyExistsException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *OperationIdAlreadyExistsException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *OperationIdAlreadyExistsException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "OperationIdAlreadyExistsException"
	}
	return *e.ErrorCodeOverride
}
func (e *OperationIdAlreadyExistsException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// Another operation is currently in progress for this stack set. Only one
// operation can be performed for a stack set at a given time.
type OperationInProgressException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *OperationInProgressException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *OperationInProgressException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *OperationInProgressException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "OperationInProgressException"
	}
	return *e.ErrorCodeOverride
}
func (e *OperationInProgressException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified ID refers to an operation that doesn't exist.
type OperationNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *OperationNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *OperationNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *OperationNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "OperationNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *OperationNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// Error reserved for use by the [CloudFormation CLI]. CloudFormation doesn't return this error to
// users.
//
// [CloudFormation CLI]: https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/what-is-cloudformation-cli.html
type OperationStatusCheckFailedException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *OperationStatusCheckFailedException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *OperationStatusCheckFailedException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *OperationStatusCheckFailedException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ConditionalCheckFailed"
	}
	return *e.ErrorCodeOverride
}
func (e *OperationStatusCheckFailedException) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

// A resource scan is currently in progress. Only one can be run at a time for an
// account in a Region.
type ResourceScanInProgressException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *ResourceScanInProgressException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *ResourceScanInProgressException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *ResourceScanInProgressException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ResourceScanInProgress"
	}
	return *e.ErrorCodeOverride
}
func (e *ResourceScanInProgressException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The limit on resource scans has been exceeded. Reasons include:
//
//   - Exceeded the daily quota for resource scans.
//
//   - A resource scan recently failed. You must wait 10 minutes before starting a
//     new resource scan.
//
//   - The last resource scan failed after exceeding 100,000 resources. When this
//     happens, you must wait 24 hours before starting a new resource scan.
type ResourceScanLimitExceededException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *ResourceScanLimitExceededException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *ResourceScanLimitExceededException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *ResourceScanLimitExceededException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ResourceScanLimitExceeded"
	}
	return *e.ErrorCodeOverride
}
func (e *ResourceScanLimitExceededException) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

// The resource scan was not found.
type ResourceScanNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *ResourceScanNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *ResourceScanNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *ResourceScanNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "ResourceScanNotFound"
	}
	return *e.ErrorCodeOverride
}
func (e *ResourceScanNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified stack instance doesn't exist.
type StackInstanceNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *StackInstanceNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *StackInstanceNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *StackInstanceNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "StackInstanceNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *StackInstanceNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified stack ARN doesn't exist or stack doesn't exist corresponding to
// the ARN in input.
type StackNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *StackNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *StackNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *StackNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "StackNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *StackNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// You can't yet delete this stack set, because it still contains one or more
// stack instances. Delete all stack instances from the stack set before deleting
// the stack set.
type StackSetNotEmptyException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *StackSetNotEmptyException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *StackSetNotEmptyException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *StackSetNotEmptyException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "StackSetNotEmptyException"
	}
	return *e.ErrorCodeOverride
}
func (e *StackSetNotEmptyException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified stack set doesn't exist.
type StackSetNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *StackSetNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *StackSetNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *StackSetNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "StackSetNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *StackSetNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// Another operation has been performed on this stack set since the specified
// operation was performed.
type StaleRequestException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *StaleRequestException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *StaleRequestException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *StaleRequestException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "StaleRequestException"
	}
	return *e.ErrorCodeOverride
}
func (e *StaleRequestException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// A client request token already exists.
type TokenAlreadyExistsException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *TokenAlreadyExistsException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *TokenAlreadyExistsException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *TokenAlreadyExistsException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "TokenAlreadyExistsException"
	}
	return *e.ErrorCodeOverride
}
func (e *TokenAlreadyExistsException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

// The specified extension configuration can't be found.
type TypeConfigurationNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *TypeConfigurationNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *TypeConfigurationNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *TypeConfigurationNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "TypeConfigurationNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *TypeConfigurationNotFoundException) ErrorFault() smithy.ErrorFault {
	return smithy.FaultClient
}

// The specified extension doesn't exist in the CloudFormation registry.
type TypeNotFoundException struct {
	Message *string

	ErrorCodeOverride *string

	noSmithyDocumentSerde
}

func (e *TypeNotFoundException) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}
func (e *TypeNotFoundException) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}
func (e *TypeNotFoundException) ErrorCode() string {
	if e == nil || e.ErrorCodeOverride == nil {
		return "TypeNotFoundException"
	}
	return *e.ErrorCodeOverride
}
func (e *TypeNotFoundException) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }
