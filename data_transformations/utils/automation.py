from dagster import AutomationCondition


def get_default_automation_condition():
    return (
        AutomationCondition.newly_missing()
        | AutomationCondition.code_version_changed()
        | AutomationCondition.any_deps_updated()
    ) & ~(
        AutomationCondition.in_progress()
        | AutomationCondition.any_deps_in_progress()
        | AutomationCondition.any_deps_missing()
    )
