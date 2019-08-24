import common

from plot.plotter import Plotter

def test():
    base_dev_dict = {"PM25": {
      "YSRDAQ070000000067": ["PM25_2"],
      "YSRDPM250000002891": ["PM25_1"],
      "YSRDPM10P500000085": ["PM25_1"],
      "YSRDAQ07HW00000589": ["PM25_2", "PM25_3"]
    },
    "PM10":{"":['PM10_3']}}

    plotter = Plotter()
    plotter.plot_overall_orig_data('PM25', base_dev_dict['PM25'], ['PM25_3'], 'processed')

test()