"""
Path plot testing module, written after issues with severe path complexity emerged.
"""
import mplleaflet
import matplotlib.pyplot as plt


def plot(coord_list):
    plt.plot([coord[1] for coord in coord_list], [coord[0] for coord in coord_list])
    mplleaflet.show()
