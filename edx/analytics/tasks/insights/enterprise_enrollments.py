"""Compute metrics related to user enrollments in courses"""

import datetime
import logging

import luigi
import luigi.task

from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartitionTask, OverwriteAwareHiveQueryDataTask, WarehouseMixin
)
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.record import BooleanField, DateField, DateTimeField, IntegerField, Record, StringField
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserTask,
    ImportEnterpriseCustomerTask,
    ImportEnterpriseCustomerUserTask,
    ImportEnterpriseCourseEnrollmentUserTask,
    ImportDataSharingConsentTask,
    ImportStudentCourseEnrollmentTask,
    ImportPersistentCourseGradeTask,
)
from edx.analytics.tasks.warehouse.load_internal_reporting_course_catalog import CoursePartitionTask
from edx.analytics.tasks.warehouse.load_internal_reporting_user import AggregateInternalReportingUserTableHive


class EnterpriseEnrollmentRecord(Record):
    """Summarizes a course's enrollment by gender and date."""
    enterprise_id = StringField(description='')
    enterprise_name = StringField(description='')
    lms_user_id = IntegerField(description='')
    enterprise_user_id = IntegerField(description='')
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    enrollment_created_timestamp = DateTimeField(description='')
    user_current_enrollment_mode = StringField(description='')
    consent_granted = BooleanField(description='')
    letter_grade = StringField(description='')
    has_passed = BooleanField(description='')
    passed_timestamp = DateTimeField(description='')
    enterprise_sso_uid = StringField(description='')
    enterprise_site_id = IntegerField(description='')
    course_title = StringField(description='')
    course_start = DateTimeField(description='')
    course_end = DateTimeField(description='')
    course_pacing_type = StringField(description='')
    course_duration_weeks = IntegerField(description='')
    course_min_effort = IntegerField(description='')
    course_max_effort = IntegerField(description='')
    user_account_creation_timestamp = DateTimeField(description='')
    user_email = StringField(description='')
    user_username = StringField(description='')
    user_age = IntegerField(description='')
    user_level_of_education = StringField(description='')
    user_gender = StringField(description='The gender of the learner.')
    user_country_code = StringField(description='')


class EnterpriseEnrollmentHiveTableTask(BareHiveTableTask):
    """
    Creates the metadata for the course_enrollment_gender_daily hive table

    Creates the Hive table in the local Hive environment.  This is just a descriptor, and does not require any data to
    be present or real.
    """
    @property  # pragma: no cover
    def partition_by(self):
        return 'dt'

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_enrollment'

    @property
    def columns(self):
        return EnterpriseEnrollmentRecord.get_hive_schema()


class EnterpriseEnrollmentHivePartitionTask(HivePartitionTask):
    """
    Generates the course_enrollment_gender_daily hive partition.
    """
    date = luigi.DateParameter()

    @property
    def hive_table_task(self):  # pragma: no cover
        return EnterpriseEnrollmentHiveTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite
        )

    @property
    def partition_value(self):  # pragma: no cover
        """ Use a dynamic partition value based on the date parameter. """
        return self.date.isoformat()  # pylint: disable=no-member


class EnterpriseEnrollmentDataTask(OverwriteAwareHiveQueryDataTask):
    """
    Executes a hive query to summarize enrollment data and store it in the course_enrollment_gender_daily hive table.
    """

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        return """
            SELECT
                enterprise_customer.uuid,
                enterprise_customer.name,
                enterprise_user.user_id,
                enterprise_user.id,
                enterprise_course_enrollment.course_id,
                enterprise_course_enrollment.created,
                enrollment.mode,
                consent.granted,
                grades.letter_grade,
                CASE
                    WHEN grades.passed_timestamp IS NULL THEN 0
                    ELSE 1
                END AS has_passed,
                grades.passed_timestamp,
                right(social_auth.uid_full, character_length(social_auth.uid_full)-position(':' in social_auth.uid_full using characters)) AS enterprise_sso_uid,
                enterprise_customer.site_id,
                course.course_title,
                course.course_start,
                course.course_end,
                course.course_pacing_type,
                CASE
                    WHEN course.course_pacing_type = 'self_paced' THEN 'Self Paced'
                    ELSE TO_CHAR(DATEDIFF('week', course.course_start, course.course_end))
                END AS course_duration,
                course.course_min_effort,
                course.course_max_effort,
                user_profile.date_joined,
                user_profile.email,
                user_profile.username,
                YEAR(CURRENT_DATE() - user_profile.year_of_birth) AS user_age,
                user_profile.level_of_education,
                user_profile.gender,
                user_profile.last_country,
            FROM
                enterprise_enterprisecourseenrollment enterprise_course_enrollment
            JOIN 
                enterprise_enterprisecustomeruser enterprise_user
            ON
                enterprise_course_enrollment.enterprise_customer_user_id = enterprise_user.id
            JOIN
                enterprise_enterprisecustomer enterprise_customer
            ON
                enterprise_user.enterprise_customer_id = enterprise_customer.uuid
            JOIN
                student_courseenrollment enrollment
            ON
                enterprise_course_enrollment.course_id = enrollment.course_id
            AND
                enterprise_user.user_id = enrollment.user_id
            JOIN
                auth_user
            ON 
                enterprise_user.user_id = auth_user.id
            JOIN
                consent_datasharingconsent consent
            ON
                auth_user.username =  consent.username
            AND 
                enterprise_course_enrollment.course_id = consent.course_id
            JOIN
                grades_persistentcoursegrade grades
            ON 
                enterprise_user.user_id = grades.user_id
            AND 
                enterprise_course_enrollment.course_id = grades.course_id
            JOIN
                (
                    SELECT
                        user_id,
                        provider,
                        MAX(uid) AS uid_full
                    FROM
                        social_auth_usersocialauth
                    WHERE
                        provider = 'tpa-saml'
                    GROUP BY
                        1, 2	
                ) social_auth
            ON 
                enterprise_user.user_id = social_auth.user_id
            JOIN
                course_catalog course
            ON 
                enterprise_course_enrollment.course_id = course.course_id
            JOIN
                internal_reporting_user user_profile
            ON
                enterprise_user.user_id = user_profile.user_id
        """

    @property
    def hive_partition_task(self):  # pragma: no cover
        """The task that creates the partition used by this job."""
        return EnterpriseEnrollmentHivePartitionTask(
            date=self.interval.date_b,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    def requires(self):  # pragma: no cover
        for requirement in super(EnterpriseEnrollmentDataTask, self).requires():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            ImportAuthUserTask(),
            ImportEnterpriseCustomerTask(),
            ImportEnterpriseCustomerUserTask(),
            ImportEnterpriseCourseEnrollmentUserTask(),
            ImportDataSharingConsentTask(),
            ImportStudentCourseEnrollmentTask(),
            ImportPersistentCourseGradeTask(),
            CoursePartitionTask(
                date=self.date,
                warehouse_path=self.warehouse_path,
                api_root_url=self.api_root_url,
                api_page_size=self.api_page_size,
            ),
            AggregateInternalReportingUserTableHive(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
        )


class EnterpriseEnrollmentMysqlTask(MysqlInsertTask):
    """
    Breakdown of enrollments by gender as reported by the user.

    During operations: The object at insert_source_task is opened and each row is treated as a row to be inserted.
    At the end of this task data has been written to MySQL.  Overwrite functionality is complex and configured through
    the OverwriteHiveAndMysqlDownstreamMixin, so we default the standard overwrite parameter to None.
    """
    overwrite = None

    def __init__(self, *args, **kwargs):
        super(EnterpriseEnrollmentMysqlTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql

    @property
    def table(self):  # pragma: no cover
        return 'enterprise_enrollment'

    @property
    def insert_source_task(self):  # pragma: no cover
        return EnterpriseEnrollmentDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            overwrite_n_days=self.overwrite_n_days,
            overwrite=self.overwrite_hive,
        )

    @property
    def columns(self):
        return EnterpriseEnrollmentRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('enterprise_id',),
        ]


@workflow_entry_point
class ImportEnterpriseEnrollmentsIntoMysql(luigi.WrapperTask):
    """Import enterprise enrollment data into MySQL."""

    def requires(self):
        enrollment_kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
            'overwrite_n_days': self.overwrite_n_days,
            'overwrite_hive': self.overwrite_hive,
            'overwrite_mysql': self.overwrite_mysql,
        }

        course_summary_kwargs = dict({
            'date': self.date,
            'api_root_url': self.api_root_url,
            'api_page_size': self.api_page_size,
            'enable_course_catalog': self.enable_course_catalog,
        }, **enrollment_kwargs)

        course_enrollment_summary_args = dict({
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
            'overwrite_n_days': self.overwrite_n_days,
            'overwrite': self.overwrite_hive,
        })

        yield [
            EnterpriseEnrollmentMysqlTask(),
        ]
