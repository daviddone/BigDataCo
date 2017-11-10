package index;

public enum IndexMdtSourceEnum {
	TIMESTAMP(0),
	ENBID(1),
	MMEGROUPID(2),
	MMECODE(3),
	IMSI(4),
	IMEI(5),
	MMEUES1APID(6),
	CELLID(7),
	SCPCI(8),
	SCFREQ(9),
	SCRSRP(10),
	SCRSRQ(11),
	NC1PCI(12),
	NC1FREQ(13),
	NC1RSRP(14),
	NC1RSRQ(15),
	NC2PCI(16),
	NC2FREQ(17),
	NC2RSRP(18),
	NC2RSRQ(19),
	NC3PCI(20),
	NC3FREQ(21),
	NC3RSRP(22),
	NC3RSRQ(23),
	NC4PCI(24),
	NC4FREQ(25),
	NC4RSRP(26),
	NC4RSRQ(27),
	NC5PCI(28),
	NC5FREQ(29),
	NC5RSRP(30),
	NC5RSRQ(31),
	NC6PCI(32),
	NC6FREQ(33),
	NC6RSRP(34),
	NC6RSRQ(35),
	LONGITUDE(36),
	LATITUDE(37);

	private int index;

	IndexMdtSourceEnum(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
}
