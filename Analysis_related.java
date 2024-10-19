import java.util.ArrayList;
import java.util.List;

public class Analysis_related {
    public static void main(String[] args) {
        // 数据
        double[] rates = {4.5, 5.5, 6.5}; // 使用区间的中间值
        double[] inflows = {159365440, 234931629, 378151919};
        double[] outflows = {152664975, 189426341, 195203850};

        // 计算相关系数
        double correlationInflow = calculateCorrelation(rates, inflows);
        double correlationOutflow = calculateCorrelation(rates, outflows);

        System.out.println("利率与资金流入量的相关系数: " + correlationInflow);
        System.out.println("利率与资金流出量的相关系数: " + correlationOutflow);
    }

    public static double calculateCorrelation(double[] x, double[] y) {
        int n = x.length;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;

        for (int i = 0; i < n; i++) {
            sumX += x[i];
            sumY += y[i];
            sumXY += x[i] * y[i];
            sumX2 += x[i] * x[i];
            sumY2 += y[i] * y[i];
        }

        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        return numerator / denominator;
    }
}
