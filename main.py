"""
Main class that analyzes repository statistics
"""
import sys
from json import loads, dumps
from kafka import KafkaConsumer


def create_kafka_consumer(topic: str = 'fasten.RepoAnalyzerExtension.out',
                          bootstrap_server: str = 'localhost:9092'):
    """
    Creates a Kafka consumer for a given topic
    :param topic: Topic name to consume from
    :param bootstrap_server: Kafka Bootstrap Server address
    :return: KafkaConsumer
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='fasten.RepoCodeStatistics.consumerGroup',
        value_deserializer=lambda x: loads(x.decode('utf-8')))


def get_statistics(consumer: KafkaConsumer):
    """
    Consumes all messages provided by the given consumer
    and aggregates statistics about analyzed repositories
    :param consumer: KafkaConsumer
    :return: repositories statistics
    """
    build_managers = {}
    num_projects = 0.0
    avg_unit_tests_with_mocks = 0.0
    avg_files_with_mock_imports = 0.0
    avg_source_files = 0.0
    avg_number_of_methods = 0.0
    avg_number_of_unit_tests = 0.0
    avg_test_files = 0.0
    try:
        for message in consumer:
            payload = message.value['payload']
            num_projects += 1.0
            build_manager = payload['buildManager']
            if build_manager in build_managers:
                build_managers[build_manager] = build_managers[build_manager] + 1
            else:
                build_managers[build_manager] = 1
            modules = payload['modules']
            for module in modules:
                avg_unit_tests_with_mocks += float(module['unitTestsWithMocks'])
                avg_files_with_mock_imports += float(module['filesWithMockImport'])
                avg_source_files += float(module['sourceFiles'])
                avg_number_of_methods += float(module['numberOfFunctions'])
                avg_number_of_unit_tests += float(module['numberOfUnitTests'])
                avg_test_files += float(module['testFiles'])
            print('Projects processed:', int(num_projects))
    except KeyBoardInterrupt:
        print('Processing interrupted after', int(num_projects), 'messages')
    avg_unit_tests_with_mocks /= num_projects
    avg_files_with_mock_imports /= num_projects
    avg_source_files /= num_projects
    avg_number_of_methods /= num_projects
    avg_number_of_unit_tests /= num_projects
    avg_test_files /= num_projects
    return {
        'num_projects': int(num_projects),
        'build_managers': build_managers,
        'avg_unit_tests_with_mocks': avg_unit_tests_with_mocks,
        'avg_files_with_mock_imports': avg_files_with_mock_imports,
        'avg_source_files': avg_source_files,
        'avg_number_of_methods': avg_number_of_methods,
        'avg_number_of_unit_tests': avg_number_of_unit_tests,
        'avg_test_files': avg_test_files
    }


def main():
    """
    Main method that creates Kafka consumer
    and gathers statistics from Kafka topic
    :return: None
    """
    if len(sys.argv) == 1:
        consumer = create_kafka_consumer()
    elif len(sys.argv) == 2:
        consumer = create_kafka_consumer(sys.argv[1])
    elif len(sys.argv) == 3:
        consumer = create_kafka_consumer(sys.argv[1], sys.argv[2])
    else:
        print('Incorrect arguments!')
        return
    stats = get_statistics(consumer)
    print(dumps(stats))


if __name__ == '__main__':
    main()
