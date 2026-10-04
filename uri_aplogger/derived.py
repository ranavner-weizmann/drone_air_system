# derived.py
"""
Derived quantities computed per data row (PM1/PM2.5 for POPS, Angstrom
exponent fit for the MA200, scaled iMet temperatures).

Design constraints: this runs inside the real-time logging loop on a
Raspberry Pi, at the sensors' native rate. Everything here is pure Python
with no per-row allocation beyond a few floats, and every constant that can
be precomputed is precomputed once at construction. No numpy: for 5-16
element vectors the numpy call overhead dominates and plain float math is
several times faster.
"""

import math


def to_float(s):
    """float(s) or None, never raises."""
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def fmt(x, sig=5):
    """Format a derived float with `sig` significant digits; None -> ''."""
    return "" if x is None else f"{x:.{sig}g}"


# ---------------------------------------------------------------------------
# POPS: PM1 / PM2.5 from the size histogram
# ---------------------------------------------------------------------------

# Optical-diameter bin edges in nm, taken from the POPS User Manual Rev. 8,
# Appendix 3 "Determining size bin boundaries" (Handix Scientific). The
# conversion from scattering amplitude to diameter is Mie theory for PSL
# (refractive index 1.615+0.001i) and assumes the factory histogram settings
# logmin = 1.6, logmax = 4.817. There are nbins+1 edges per table.
POPS_BIN_EDGES_NM = {
    8: [115, 135, 165, 210, 350, 575, 1220, 1990, 3370],
    16: [115, 125, 135, 150, 165, 185, 210, 250, 350, 475, 575,
         855, 1220, 1530, 1990, 2585, 3370],
}
POPS_FACTORY_LOGMIN = 1.6
POPS_FACTORY_LOGMAX = 4.817


class PopsPMCalculator:
    """
    Mass concentration below a set of diameter cutoffs from POPS bin counts.

    Each particle in a bin is treated as a sphere with the bin's geometric
    mean diameter. A bin that straddles a cutoff contributes the fraction of
    its log-width that lies below the cutoff, using the geometric mean of
    that sub-range. Counts are per second (POPS histograms are 1 s), so the
    sampled volume is flow [cm3/s] x 1 s.

        mass per particle [ug] = rho[g/cm3] * pi/6 * (D[nm] * 1e-7)^3 * 1e6
        concentration [ug/m3]  = sum(mass_i * N_i) / V[cm3] * 1e6

    Both unit factors fold into one constant: rho * pi/6 * D^3 * 1e-9.
    """

    def __init__(self, density_g_cm3=1.65, cutoffs_nm=(1000.0, 2500.0)):
        self.density = float(density_g_cm3)
        self.cutoffs = tuple(float(c) for c in cutoffs_nm)
        self._coef_cache = {}

    def coefficients(self, nbins):
        """Per-bin mass coefficients for each cutoff, or None if nbins has no table."""
        coefs = self._coef_cache.get(nbins)
        if coefs is None:
            edges = POPS_BIN_EDGES_NM.get(nbins)
            if edges is None:
                return None
            coefs = tuple(self._build(edges, c) for c in self.cutoffs)
            self._coef_cache[nbins] = coefs
        return coefs

    def _build(self, edges, cutoff):
        k = self.density * math.pi / 6.0 * 1e-9
        coefs = []
        for lo, hi in zip(edges[:-1], edges[1:]):
            if hi <= cutoff:
                dg = math.sqrt(lo * hi)
                coefs.append(k * dg * dg * dg)
            elif lo < cutoff:
                frac = math.log(cutoff / lo) / math.log(hi / lo)
                dg = math.sqrt(lo * cutoff)
                coefs.append(frac * k * dg * dg * dg)
            else:
                coefs.append(0.0)
        return coefs

    def compute(self, counts, nbins, flow_cm3_s, sample_time_s=1.0):
        """
        counts: per-bin counts (strings or numbers), at least nbins long.
        Returns a tuple of concentrations [ug/m3] (one per cutoff); each entry
        is None if the inputs do not allow a value.
        """
        n_out = len(self.cutoffs)
        coefs = self.coefficients(nbins)
        if coefs is None or flow_cm3_s is None or flow_cm3_s <= 0:
            return (None,) * n_out
        if len(counts) < nbins:
            return (None,) * n_out

        inv_vol = 1.0 / (flow_cm3_s * sample_time_s)
        totals = [0.0] * n_out
        for i in range(nbins):
            n = to_float(counts[i])
            if n is None or n <= 0:
                continue
            for j in range(n_out):
                totals[j] += coefs[j][i] * n
        return tuple(t * inv_vol for t in totals)


# ---------------------------------------------------------------------------
# MA200: absorption Angstrom exponent fit
# ---------------------------------------------------------------------------

# Channel wavelengths of the MA200 (AethLabs manual, section 1).
MA200_WAVELENGTHS_NM = {"UV": 375.0, "blue": 470.0, "green": 528.0,
                        "red": 625.0, "IR": 880.0}

# Mass absorption cross-sections (sigma_ATN, m2/g) the MA series uses to
# turn attenuation into BC mass. babs = BC * MAC recovers the absorption
# coefficient the fit should be done on. Overridable from sensor_config.json.
MA200_MAC_M2_G = {"UV": 24.069, "blue": 19.070, "green": 17.028,
                  "red": 14.091, "IR": 10.120}

MA200_CHANNEL_ORDER = ("UV", "blue", "green", "red", "IR")


def bc_to_babs_Mm(bc_ng_m3, mac_m2_g):
    """Black-carbon mass [ng/m3] -> absorption coefficient [Mm^-1]."""
    # ng/m3 * 1e-9 g/ng * m2/g = m^-1 ; * 1e6 = Mm^-1
    return bc_ng_m3 * mac_m2_g * 1e-3


class PowerLawFit:
    """
    Least-squares fit of  y = A * (lam / lam_ref)^(-alpha)  in log-log space.

    For the aethalometer, y is babs and alpha is the absorption Angstrom
    exponent (AAE); A is the fitted absorption coefficient at lam_ref, i.e.
    the amplitude of the power law. log(lam/lam_ref) is precomputed; per call
    the cost is one log per usable channel plus a handful of multiplies.
    Channels with missing or non-positive values are dropped from the fit.
    """

    def __init__(self, wavelengths_nm, ref_wavelength_nm):
        self.ref = float(ref_wavelength_nm)
        self.x = [math.log(float(w) / self.ref) for w in wavelengths_nm]

    def fit(self, values):
        """values aligned with wavelengths. Returns (alpha, amplitude) or (None, None)."""
        xs = []
        ys = []
        for x, v in zip(self.x, values):
            if v is not None and v > 0:
                xs.append(x)
                ys.append(math.log(v))
        n = len(xs)
        if n < 2:
            return None, None
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = 0.0
        sxy = 0.0
        for x, y in zip(xs, ys):
            dx = x - mx
            sxx += dx * dx
            sxy += dx * (y - my)
        if sxx == 0.0:
            return None, None
        slope = sxy / sxx
        intercept = my - slope * mx
        return -slope, math.exp(intercept)
