import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import powerlaw     # DOCS: https://pythonhosted.org/powerlaw/


def drawBoxPlots(data, xlabels, savingPath, **kargs):
    # This is lame and ugly, the seaborn ones are much prefered.
    positions = np.arange(len(data)) + 1

    fig = plt.figure(figsize=(10, 10))
    ax = plt.axes()

    # plt.xticks(positions, xlabels)
    bp = ax.boxplot(data, positions=positions, showmeans=True, **kargs)

    ax.set_xticklabels(xlabels, rotation=45, ha='right')
    plt.savefig(savingPath)
    # plt.show()
    plt.close()
    return plt


def degreeDistWithPowerLaw(degrees, fileName):

    # The theory behind drawing power-laws: http://networksciencebook.com/chapter/4/#advanced-b

    fig = plt.figure()
    ax = plt.axes()

    values = pd.Series(degrees).sort_values().values
    numBins = np.unique(values).shape[0]
    n, bins, patches = ax.hist(values, numBins)
    plt.yscale('log')

    fit = powerlaw.Fit(values)

    fit.power_law.plot_pdf(ax=ax)   # Plots the probability density function (PDF) of the theoretical distribution for the values given in data
    fit.plot_pdf()  # Plots the probability density function (PDF) (pretty linear)

    '''
    fit.power_law.plot_cdf(survival=True, ax=ax)   # Plots the probability density function (PDF) of the theoretical distribution for the values given in data
    fit.plot_cdf(survival=True)  # Plots the cumulative degree distribution
    '''

    ax.legend([f'PDF of  theoretical distribution', 'PDF (gamma={:.3f})'.format(fit.power_law.alpha)])
    # print('alpha= ', fit.power_law.alpha, '  sigma= ', fit.power_law.sigma)

    plt.savefig(fileName + ".png")
    plt.close()

    return plt