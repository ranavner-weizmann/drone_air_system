"""
Tests for the derived columns (POPS PM1/PM2.5, MA200 AAE fit, iMet scaling).

Run from uri_aplogger/ with:   python -m unittest tests.test_derived -v

The sensor-class tests stub out pyudev/serial if they are not installed so
they can run on a development machine, not only on the Pi.
"""

import math
import os
import sys
import tempfile
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Keep per-run output folders out of the repo while testing.
_TMP_RUN = tempfile.mkdtemp(prefix="aplogger_test_")
os.environ["RUN_DIR"] = _TMP_RUN

for _mod in ("pyudev", "serial"):
    try:
        __import__(_mod)
    except ImportError:
        stub = types.ModuleType(_mod)
        if _mod == "serial":
            stub.Serial = object
            stub.EIGHTBITS = 8
            stub.PARITY_NONE = "N"
            stub.STOPBITS_ONE = 1
            stub.SerialException = Exception
        sys.modules[_mod] = stub

from derived import (PopsPMCalculator, PowerLawFit, POPS_BIN_EDGES_NM,  # noqa: E402
                     POPS_FACTORY_EDGES_NM, pops_bin_edges_nm,
                     MA200_WAVELENGTHS_NM, MA200_MAC_M2_G, MA200_CHANNEL_ORDER,
                     bc_to_babs_Mm, fmt, to_float)
import sensor_implementations as impl  # noqa: E402


def sphere_mass_ug_m3(d_nm, density, n_per_cm3=1.0):
    """Reference: mass concentration of n spheres/cm3 of diameter d_nm."""
    vol_cm3 = math.pi / 6.0 * (d_nm * 1e-7) ** 3
    return n_per_cm3 * density * vol_cm3 * 1e6 * 1e6


class TestPopsPM(unittest.TestCase):
    def test_single_bin_matches_sphere_mass(self):
        calc = PopsPMCalculator(density_g_cm3=1.65)
        edges = POPS_BIN_EDGES_NM[16]
        counts = [0] * 16
        counts[0] = 3          # 3 particles in 115-125 nm
        flow = 3.0             # cm3/s -> 3 cm3 sampled in 1 s
        pm1, pm25 = calc.compute(counts, 16, flow)
        dg = math.sqrt(edges[0] * edges[1])
        expected = sphere_mass_ug_m3(dg, 1.65, n_per_cm3=3 / flow)
        self.assertAlmostEqual(pm1, expected, places=12)
        self.assertAlmostEqual(pm25, expected, places=12)

    def test_cutoff_handling(self):
        calc = PopsPMCalculator(density_g_cm3=1.0)
        # bin 15 (index) is 2585-3370 nm: entirely above 2.5 um -> nothing
        counts = [0] * 16
        counts[15] = 100
        pm1, pm25 = calc.compute(counts, 16, 1.0)
        self.assertEqual(pm1, 0.0)
        self.assertEqual(pm25, 0.0)
        # bin 11 (855-1220) straddles 1 um: partial PM1, full PM2.5
        counts = [0] * 16
        counts[11] = 1
        pm1, pm25 = calc.compute(counts, 16, 1.0)
        self.assertGreater(pm1, 0.0)
        self.assertLess(pm1, pm25)
        frac = math.log(1000 / 855) / math.log(1220 / 855)
        dg_sub = math.sqrt(855 * 1000)
        self.assertAlmostEqual(pm1, frac * sphere_mass_ug_m3(dg_sub, 1.0), places=12)

    def test_pm1_never_exceeds_pm25(self):
        calc = PopsPMCalculator()
        counts = [str(50 - 3 * i) for i in range(16)]
        pm1, pm25 = calc.compute(counts, 16, 2.9)
        self.assertLessEqual(pm1, pm25)

    def test_unsupported_inputs_give_none(self):
        calc = PopsPMCalculator()
        self.assertEqual(calc.compute([1] * 16, 0, 3.0), (None, None))    # no bins
        self.assertEqual(calc.compute([1] * 16, 16, 3.0, 4.8, 1.0), (None, None))  # logmax <= logmin
        self.assertEqual(calc.compute([1] * 16, 16, 0.0), (None, None))   # zero flow
        self.assertEqual(calc.compute([1] * 16, 16, None), (None, None))  # missing flow
        self.assertEqual(calc.compute([1] * 4, 16, 3.0), (None, None))    # short row

    def test_edges_reproduce_manual_tables(self):
        e16 = pops_bin_edges_nm(16, 1.6, 4.817)
        for got, want in zip(e16, POPS_FACTORY_EDGES_NM):
            self.assertAlmostEqual(got, want, places=9)
        e8 = pops_bin_edges_nm(8, 1.6, 4.817)
        for got, want in zip(e8, POPS_BIN_EDGES_NM[8]):
            self.assertAlmostEqual(got, want, places=9)

    def test_edges_for_field_config(self):
        # Our unit reports logmin=1.0, logmax=4.81 (manual table assumes 1.6/4.817)
        e = pops_bin_edges_nm(16, 1.0, 4.81)
        self.assertEqual(len(e), 17)
        self.assertTrue(all(b > a for a, b in zip(e, e[1:])))   # monotonic
        self.assertLess(e[0], 115)                                # extrapolated below the table
        self.assertAlmostEqual(e[-1], 3370, delta=40)             # top edge ~ factory top
        # amplitude 10^1.6 must map to 115 nm regardless of the grid it sits on
        e_single = pops_bin_edges_nm(1, 1.6, 4.817)
        self.assertAlmostEqual(e_single[0], 115, places=9)
        # a 12-bin configuration is now computable
        self.assertIsNotNone(PopsPMCalculator().coefficients(12, 1.0, 4.81))

    def test_eight_bin_table(self):
        calc = PopsPMCalculator()
        pm1, pm25 = calc.compute(["1"] * 8, 8, 3.0)
        self.assertGreater(pm25, pm1)


class TestPowerLawFit(unittest.TestCase):
    def test_recovers_exact_power_law(self):
        lams = [MA200_WAVELENGTHS_NM[c] for c in MA200_CHANNEL_ORDER]
        fit = PowerLawFit(lams, 880.0)
        A, alpha = 12.5, 1.37
        vals = [A * (l / 880.0) ** (-alpha) for l in lams]
        a_fit, amp_fit = fit.fit(vals)
        self.assertAlmostEqual(a_fit, alpha, places=10)
        self.assertAlmostEqual(amp_fit, A, places=9)

    def test_skips_bad_channels(self):
        lams = [375.0, 470.0, 528.0, 625.0, 880.0]
        fit = PowerLawFit(lams, 880.0)
        vals = [None, 5.0 * (470 / 880.0) ** -2.0, -3.0, 5.0 * (625 / 880.0) ** -2.0, 5.0]
        a_fit, amp_fit = fit.fit(vals)
        self.assertAlmostEqual(a_fit, 2.0, places=10)
        self.assertAlmostEqual(amp_fit, 5.0, places=10)
        self.assertEqual(fit.fit([None, None, None, None, 5.0]), (None, None))

    def test_bc_to_babs(self):
        # 1000 ng/m3 of BC at 880 nm with MAC 10.12 m2/g -> 10.12 Mm^-1
        self.assertAlmostEqual(bc_to_babs_Mm(1000.0, MA200_MAC_M2_G["IR"]), 10.12, places=12)


class TestHelpers(unittest.TestCase):
    def test_fmt_and_to_float(self):
        self.assertEqual(fmt(None), "")
        self.assertEqual(fmt(1.23456789, 4), "1.235")
        self.assertIsNone(to_float(""))
        self.assertIsNone(to_float("abc"))
        self.assertEqual(to_float("+2268"), 2268.0)


# ---------------------------------------------------------------------------
# Sensor classes end-to-end on representative lines
# ---------------------------------------------------------------------------

def _load_config():
    import json
    with open(os.path.join(ROOT, "sensor_config.json"), encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg["sensors"]


def _quiet(cfg):
    cfg = dict(cfg)
    cfg["logging"] = {"verbosity": 0}
    return cfg


class TestSensorRows(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sensors = _load_config()

    def _check_shape(self, row, cols):
        self.assertEqual(len(row), len(cols), f"row/header length mismatch:\n{row}\n{cols}")

    def test_imet_row(self):
        cfg = _quiet(self.sensors["imet"])
        s = impl.iMetSensor("imet", cfg)
        line = "XQ,+098168,+2268,+0499,+2383,2015/10/18,02:29:07,-855702219,+428939479,+00242872,00"
        row = s.parse_data(line)
        self._check_shape(row, cfg["column_names"])
        d = dict(zip(cfg["column_names"], row))
        self.assertEqual(d["temp"], "+2268")
        self.assertEqual(d["temp_C"], "22.68")
        self.assertEqual(d["hum_temp_C"], "23.83")
        self.assertEqual(d["pressure"], 981.68)
        self.assertEqual(d["rel_hum"], 49.9)

    def test_ma200_row_from_manual_example(self):
        cfg = _quiet(self.sensors["miniaeth"])
        s = impl.MiniaethMA200Sensor("miniaeth", cfg)
        # Version 2 line from the MA200 operating manual (section 5.8.6 example).
        line = ("MA200-0011,25157,18,1,1.08,2018-12-06T20:29:01.00,-480,37.746172547,-122.420371919,"
                "0.146,60,64,100,342,-611,-24357,1,100.00,99.99,58.56,41.43,32.95,23.97,9.69,100596.00,"
                "33.62,DS-UV-B-G-R-IR,681907,622183,917573,25.9730,18.4158,-0.0198,780829,620878,736176,"
                "19.5300,13.7320,-0.0103,781964,625168,706936,16.6708,11.6571,0.0168,806763,690497,782392,"
                "13.4350,9.3264,-0.0353,675814,773992,951767,9.4010,6.3921,-0.1671,"
                "415,374,274,396,376,330,427,478,594,377,340,255,510,410,198,5D91")
        row = s.parse_data(line)
        self._check_shape(row, cfg["column_names"])
        d = dict(zip(cfg["column_names"], row))
        self.assertEqual(d["UV_BCc"], "274")
        self.assertEqual(d["IR_BCc"], "198")
        # Independent reference fit on BCc * MAC
        bcc = {"UV": 274, "blue": 330, "green": 594, "red": 255, "IR": 198}
        lams = [MA200_WAVELENGTHS_NM[c] for c in MA200_CHANNEL_ORDER]
        babs = [bc_to_babs_Mm(bcc[c], MA200_MAC_M2_G[c]) for c in MA200_CHANNEL_ORDER]
        ref_aae, ref_amp = PowerLawFit(lams, 880.0).fit(babs)
        self.assertEqual(d["AAE_fit"], fmt(ref_aae, 4))
        self.assertEqual(d["AAE_fit_amp"], fmt(ref_amp, 5))
        self.assertGreater(float(d["AAE_fit"]), 0.0)

    def test_ma200_singlespot_falls_back_to_bc1(self):
        cfg = _quiet(self.sensors["miniaeth"])
        s = impl.MiniaethMA200Sensor("miniaeth", cfg)
        cols = cfg["column_names"]
        n_raw = len(cols) - 2
        parts = [""] * (n_raw - 1)          # device fields without timestamp
        for ch, val in (("UV", 500), ("blue", 400), ("green", 350), ("red", 300), ("IR", 200)):
            parts[cols.index(f"{ch}_BC1") - 1] = str(val)   # BCc left empty
        row = s.parse_data(",".join(parts))
        d = dict(zip(cols, row))
        self.assertNotEqual(d["AAE_fit"], "")
        self.assertNotEqual(d["AAE_fit_amp"], "")

    def test_pops_row(self):
        cfg = _quiet(self.sensors["pops"])
        s = impl.POPSSensor("pops", cfg)
        cols = cfg["column_names"]
        n_raw = len(cols) - 2
        values = [""] * (n_raw - 1)         # fields after the 3 header tokens
        def put(name, v):
            values[cols.index(name) - 1] = str(v)
        put("DateTime", "1700000000.0"); put("PartCt", "120"); put("PartCon", "40.0")
        put("POPS_Flow", "3.0"); put("nbins", "16"); put("logmin", "1.6"); put("logmax", "4.817")
        counts = [20, 18, 16, 14, 12, 10, 8, 6, 5, 4, 3, 2, 1, 1, 0, 0]
        for i, c in enumerate(counts):
            put(f"b{i}", c)
        packet = "hdr0,hdr1,hdr2," + ",".join(values)
        row = s.parse_data(packet)
        self._check_shape(row, cols)
        d = dict(zip(cols, row))
        ref1, ref25 = PopsPMCalculator(1.65).compute(counts, 16, 3.0)
        self.assertEqual(d["PM1_ug_m3"], fmt(ref1, 5))
        self.assertEqual(d["PM2.5_ug_m3"], fmt(ref25, 5))
        self.assertLessEqual(float(d["PM1_ug_m3"]), float(d["PM2.5_ug_m3"]))
        self.assertEqual(d["b0"], "20")

    def test_old_header_without_derived_columns_is_left_intact(self):
        cfg = _quiet(self.sensors["imet"])
        cfg["column_names"] = cfg["column_names"][:-2]       # pre-derived config
        s = impl.iMetSensor("imet", cfg)
        line = "XQ,+098168,+2268,+0499,+2383,2015/10/18,02:29:07,-855702219,+428939479,+00242872,00"
        row = s.parse_data(line)
        self._check_shape(row, cfg["column_names"])
        self.assertEqual(row[-1], "00")                       # 'sat' still last

    def test_pops_blank_when_bins_unknown(self):
        cfg = _quiet(self.sensors["pops"])
        s = impl.POPSSensor("pops", cfg)
        cols = cfg["column_names"]
        values = [""] * (len(cols) - 3)
        values[cols.index("nbins") - 1] = "0"
        values[cols.index("POPS_Flow") - 1] = "3.0"
        row = s.parse_data("a,b,c," + ",".join(values))
        d = dict(zip(cols, row))
        self.assertEqual(d["PM1_ug_m3"], "")
        self.assertEqual(d["PM2.5_ug_m3"], "")

    def test_pops_real_packet_from_pi(self):
        # Captured 2026-10-04 on the drone Pi (logmin=1.00, logmax=4.81, 16 bins).
        cfg = _quiet(self.sensors["pops"])
        s = impl.POPSSensor("pops", cfg)
        cols = cfg["column_names"]
        payload = ("20261004T100002,36002.0833,3,0,1539,1539,513.29,2233,2258,8.32,10.40,842.46,44.77,"
                   "383.10,15.46,29.50,3.00,231.65,44.18,355.60,1172.31,32.78,10.94,2.87,1.54,30000,3.0,"
                   "16,1.00,4.81,0,8,255,512,0,0,135,314,285,243,222,178,129,26,2,4,0,1,0,1")
        row = s.parse_data("h0,h1,h2," + payload)
        self._check_shape(row, cols)
        d = dict(zip(cols, row))
        self.assertEqual(d["b2"], "135")
        self.assertEqual(d["logmin"], "1.00")
        pm1, pm25 = float(d["PM1_ug_m3"]), float(d["PM2.5_ug_m3"])
        self.assertGreater(pm1, 0.0)
        self.assertLessEqual(pm1, pm25)
        self.assertLess(pm25, 500.0)          # 513 particles/cm3, mostly sub-micron


if __name__ == "__main__":
    unittest.main()
