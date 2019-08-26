import json
import os

from click.testing import CliRunner
from dagster_graphql.cli import ui

from dagster import (
    DependencyDefinition,
    InputDefinition,
    Int,
    OutputDefinition,
    PipelineDefinition,
    RepositoryDefinition,
    lambda_solid,
    seven,
)
from dagster.seven import mock
from dagster.utils import script_relative_path


@lambda_solid(input_defs=[InputDefinition('num', Int)], output_def=OutputDefinition(Int))
def add_one(num):
    return num + 1


@lambda_solid(input_defs=[InputDefinition('num', Int)], output_def=OutputDefinition(Int))
def mult_two(num):
    return num * 2


def define_csv_hello_world():
    return PipelineDefinition(
        name='math',
        solid_defs=[add_one, mult_two],
        dependencies={'add_one': {}, 'mult_two': {'num': DependencyDefinition(add_one.name)}},
    )


def define_repository():
    return RepositoryDefinition(name='test', pipeline_dict={'math': define_csv_hello_world})


def test_basic_introspection():
    query = '{ __schema { types { name } } }'

    repo_path = script_relative_path('./cli_test_repository.yaml')

    runner = CliRunner()
    result = runner.invoke(ui, ['-y', repo_path, '-t', query])

    assert result.exit_code == 0

    result_data = json.loads(result.output)
    assert result_data['data']


def test_basic_pipelines():
    query = '{ pipelines { nodes { name } } }'

    repo_path = script_relative_path('./cli_test_repository.yaml')

    runner = CliRunner()
    result = runner.invoke(ui, ['-y', repo_path, '-t', query])

    assert result.exit_code == 0

    result_data = json.loads(result.output)
    assert result_data['data']


def test_basic_variables():
    query = 'query FooBar($pipelineName: String!){ pipeline(params:{name: $pipelineName}){ name} }'
    variables = '{"pipelineName": "math"}'
    repo_path = script_relative_path('./cli_test_repository.yaml')

    runner = CliRunner()
    result = runner.invoke(ui, ['-y', repo_path, '-v', variables, '-t', query])

    assert result.exit_code == 0

    result_data = json.loads(result.output)
    assert result_data['data']


START_PIPELINE_EXECUTION_QUERY = '''
mutation ($executionParams: ExecutionParams!) {
    startPipelineExecution(executionParams: $executionParams) {
        __typename
        ... on StartPipelineExecutionSuccess {
            run {
                runId
                pipeline { name }
                logs {
                    nodes {
                        __typename
                        ... on MessageEvent {
                            message
                            level
                        }
                        ... on ExecutionStepStartEvent {
                            step { kind }
                        }
                    }
                }
            }
        }
        ... on PipelineConfigValidationInvalid {
            pipeline { name }
            errors { message }
        }
        ... on PipelineNotFoundError {
            pipelineName
        }
    }
}
'''


def test_start_execution_text():
    variables = seven.json.dumps(
        {
            'executionParams': {
                'selector': {'name': 'math'},
                'environmentConfigData': {
                    'solids': {'add_one': {'inputs': {'num': {'value': 123}}}}
                },
                'mode': 'default',
            }
        }
    )

    repo_path = script_relative_path('./repository.yaml')

    runner = CliRunner()
    result = runner.invoke(
        ui, ['-y', repo_path, '-v', variables, '-t', START_PIPELINE_EXECUTION_QUERY]
    )

    assert result.exit_code == 0

    try:
        result_data = json.loads(result.output.strip('\n').split('\n')[-1])
        assert result_data['data']
    except Exception as e:
        raise Exception('Failed with {} Exception: {}'.format(result.output, e))


def test_start_execution_file():
    variables = seven.json.dumps(
        {
            'executionParams': {
                'selector': {'name': 'math'},
                'environmentConfigData': {
                    'solids': {'add_one': {'inputs': {'num': {'value': 123}}}}
                },
                'mode': 'default',
            }
        }
    )

    repo_path = script_relative_path('./repository.yaml')
    runner = CliRunner()
    result = runner.invoke(
        ui, ['-y', repo_path, '-v', variables, '--file', script_relative_path('./execute.graphql')]
    )

    assert result.exit_code == 0

    try:
        result_data = json.loads(result.output.strip('\n').split('\n')[-1])
        assert result_data['data']
    except Exception as e:
        raise Exception('Failed with {} Exception: {}'.format(result.output, e))


def test_start_execution_predefined():
    variables = seven.json.dumps(
        {
            'executionParams': {
                'selector': {'name': 'math'},
                'environmentConfigData': {
                    'solids': {'add_one': {'inputs': {'num': {'value': 123}}}}
                },
                'mode': 'default',
            }
        }
    )

    repo_path = script_relative_path('./repository.yaml')

    runner = CliRunner()
    result = runner.invoke(ui, ['-y', repo_path, '-v', variables, '-p', 'startPipelineExecution'])

    assert result.exit_code == 0

    try:
        result_data = json.loads(result.output.strip('\n').split('\n')[-1])
        assert result_data['data']
    except Exception as e:
        raise Exception('Failed with {} Exception: {}'.format(result.output, e))


@mock.patch.dict(os.environ, {"DAGSTER_HOME": "~/dagster"})
def test_start_execution_predefined_with_logs():
    variables = seven.json.dumps(
        {
            'executionParams': {
                'selector': {'name': 'math'},
                'environmentConfigData': {
                    'solids': {'add_one': {'inputs': {'num': {'value': 123}}}}
                },
                'mode': 'default',
            }
        }
    )

    repo_path = script_relative_path('./repository.yaml')

    runner = CliRunner()
    result = runner.invoke(
        ui, ['-y', repo_path, '-v', variables, '-p', 'startPipelineExecution', '--log']
    )

    assert result.exit_code == 0
