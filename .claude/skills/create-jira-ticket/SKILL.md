---
name: create-jira-ticket
description: Creates a Jira ticket based on a description of a pipeline alert, issue, or feature request. Uses the schema defined in `jira_ticket_schema.py` to structure the ticket.
argument-hint: <description>
---

Create a Jira ticket for the following pipeline alert or issue: $ARGUMENTS

Use the schema defined in `jira_ticket_schema.py` in this skill directory to populate the ticket fields. Choose values as follows:

- **summary**: A concise, descriptive title for the issue — max 50 characters
- **description**: A detailed explanation of the issue, including relevant context, symptoms, and any known impact
- **customfield_acceptance_criteria**: Clear, testable conditions that define when the issue can be considered resolved
- **customfield_pipeline_source**: One of `credit_card_fraud_pipeline`, `another_pipeline`, or `none` — infer from context; use `none` if the issue is not related to a specific pipeline
- **customfield_relevant_files**: A list of file paths relevant to the issue — infer from context
- **customfield_status**: One of `New`, `In Human Review`, `Ready for Dev`, `Deployed`, or `Backlog` — default to `New` for freshly created tickets
- **issuetype**: One of `Bug`, `Task`, or `Spike` — infer from context (e.g. unexpected errors → Bug, investigation → Spike, planned work → Task)
- **priority**: One of `Critical`, `High`, `Medium`, `Low` — infer from severity of the alert
- **assignee_id**: One of `unassigned`, `pipeline-detector-agent`, `team-member-1`, or `team-member-2` — default to `unassigned` unless context indicates otherwise
- **reporter_id**: One of `pipeline-detector-agent`, `team-member-1`, or `team-member-2` — use `pipeline-detector-agent` for automated alerts
- **project_id**: Use the most appropriate project ID (`1000`, `1001`, or `1002`) based on context; default to `1000` if unclear

We aren't invoking the Jira API directly in this skill, so just return the structured ticket data as a JSON object matching the schema. The calling code will handle the API interaction.

## Saving tickets locally

After generating the ticket JSON, save it to the `tickets/` folder in the repo root. Create the `tickets/` directory if it does not already exist. Use the id for the file name. For example, if the generated ticket has an id of `TICKET-001`, save it as `tickets/TICKET-001.json`. This simulates the ticket creation process for this portfolio project without needing to integrate with Jira's API directly.
