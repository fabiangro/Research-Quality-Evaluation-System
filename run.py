import sys
from ResearchEvaluationSystem import EvaluationSystemApi


api = EvaluationSystemApi()

if __name__ == '__main__':
    api.execute_calls(sys.argv[1])
