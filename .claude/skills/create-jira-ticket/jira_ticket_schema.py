"""
This file defines the schema for any Jira tickets that need to be created,
whether they are auto-generated based on monitoring signals or manually submitted by users.
The schema is based on the Jira REST API for creating issues: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-post.
"""

import glob
import os
from dataclasses import dataclass, field
from enum import Enum


def _next_ticket_id(tickets_dir: str = "tickets") -> str:
    existing = glob.glob(os.path.join(tickets_dir, "TICKET-???.json"))
    if not existing:
        return "TICKET-001"
    numbers = []
    for path in existing:
        basename = os.path.basename(path)
        parts = basename.split("-")
        if len(parts) >= 2 and parts[1].split(".")[0].isdigit():
            numbers.append(int(parts[1].split(".")[0]))
    return f"TICKET-{max(numbers) + 1:03d}"


class IssueType(str, Enum):
    BUG = "Bug"
    TASK = "Task"
    SPIKE = "Spike"


class Priority(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class PipelineID(str, Enum):
    CREDIT_CARD_FRAUD_PIPELINE = "credit_card_fraud_pipeline"
    # ANOTHER_PIPELINE = "another_pipeline"


class Source(str, Enum):
    # AGENT_DETECTOR = "agent_detector"
    # PIPELINE_DETECTOR = "pipeline_detector"
    HUMAN = "human"


class ReporterID(str, Enum):
    TEAM_MEMBER_1 = "team-member-1"
    # TEAM_MEMBER_2 = "team-member-2"
    # PIPELINE_DETECTOR_AGENT = "pipeline-detector-agent"


class ProjectID(str, Enum):
    PROJECT_1 = "1000"
    # PROJECT_2 = "1001"
    # PROJECT_3 = "1002"


class CustomFieldStatus(str, Enum):
    NEW = "New"
    IN_HUMAN_REVIEW = "In Human Review"
    READY_FOR_DEV = "Ready for Dev"
    DEPLOYED = "Deployed"
    BACKLOG = "Backlog"


class AssigneeID(str, Enum):
    TASK_PR_AGENT = "task-pr-agent"
    # TEAM_MEMBER_1 = "team-member-1"
    # TEAM_MEMBER_2 = "team-member-2"
    # UNASSIGNED = "unassigned"


@dataclass
class JiraTicket:
    project_id: ProjectID
    summary: str
    assignee_id: AssigneeID
    issuetype: IssueType
    priority: Priority
    reporter_id: ReporterID
    description: str
    customfield_acceptance_criteria: str
    customfield_pipeline_source: PipelineID
    customfield_relevant_files: list[str]
    customfield_status: CustomFieldStatus = CustomFieldStatus.NEW
    customfield_feedback: str = ""
    id: str = field(default_factory=_next_ticket_id)

    MAX_SUMMARY_LENGTH = 50

    def __post_init__(self):
        if not self.summary.strip():
            raise ValueError("summary cannot be empty")
        if len(self.summary) > self.MAX_SUMMARY_LENGTH:
            raise ValueError(f"summary exceeds {self.MAX_SUMMARY_LENGTH} character limit")
        if not self.description.strip():
            raise ValueError("description cannot be empty")
        if not self.customfield_acceptance_criteria.strip():
            raise ValueError("acceptance criteria cannot be empty")
        if not self.customfield_relevant_files:
            raise ValueError("relevant files cannot be empty")

    def to_dict(self) -> dict:
        return {
            "fields": {
                "project": {"id": self.project_id},
                "summary": self.summary,
                "assignee": {"id": self.assignee_id},
                "issuetype": {"name": self.issuetype},
                "priority": {"name": self.priority},
                "reporter": {"id": self.reporter_id},
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": self.description}]}]
                },
                "customfield_acceptance_criteria": {
                    "type": "doc",
                    "version": 1,
                    "content": [{"type": "paragraph", "content": [{"type": "text", "text": self.customfield_acceptance_criteria}]}]
                },
                "customfield_pipeline_source": self.customfield_pipeline_source,
                "customfield_relevant_files": self.customfield_relevant_files,
                "customfield_status": self.customfield_status,
                "customfield_feedback": self.customfield_feedback,
            },
            "id": self.id
        }
