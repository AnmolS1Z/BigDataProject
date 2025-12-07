package edu.uchicago.quake;

public class QuakeRecord {
    private String state;
    private int year;
    private int month;
    private long quakeCount;
    private double maxMag;
    private int labelQuakeGe4;
    private double predProbGe4;

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public int getYear() { return year; }
    public void setYear(int year) { this.year = year; }

    public int getMonth() { return month; }
    public void setMonth(int month) { this.month = month; }

    public long getQuakeCount() { return quakeCount; }
    public void setQuakeCount(long quakeCount) { this.quakeCount = quakeCount; }

    public double getMaxMag() { return maxMag; }
    public void setMaxMag(double maxMag) { this.maxMag = maxMag; }

    public int getLabelQuakeGe4() { return labelQuakeGe4; }
    public void setLabelQuakeGe4(int labelQuakeGe4) { this.labelQuakeGe4 = labelQuakeGe4; }

    public double getPredProbGe4() { return predProbGe4; }
    public void setPredProbGe4(double predProbGe4) { this.predProbGe4 = predProbGe4; }
}