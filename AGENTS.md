After every meaningful implementation step, the agent must create a verified
checkpoint before continuing.

A verified checkpoint means:

1. implement the step,
2. run the smallest relevant build/tests,
3. inspect failures,
4. fix implementation-caused failures,
5. rerun verification,
6. update the implementation walkthrough,
7. record the checkpoint result.

The agent MAY continue automatically to the next previously approved step
after a checkpoint passes.

Human approval is NOT required between previously approved steps.

Stop and wait for the user only when:

- verification cannot be completed,
- an external credential/resource is required,
- a destructive or irreversible action requires confirmation,
- a new architectural decision outside the approved plan is required,
- repository state contains conflicting user work that cannot be safely
  reconciled.

Normal successful build/test checkpoints do not require human interaction.