import sys
from ResearchEvaluationSystem import EvaluationSystemApi


api = EvaluationSystemApi()

if __name__ == '__main__':
    api.execute(sys.argv[1])
