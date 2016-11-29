package com.turk.util.string;


public class LevenshteinDistance
{
	private static int min(int a, int b, int c)
	{
		int mi = a;
		if (b < mi)
		{
			mi = b;
		}
		if (c < mi)
		{
			mi = c;
		}
		return mi;
	}

	public static int ld(String s, String t)
	{
		int n = s.length();
		int m = t.length();
		if (n == 0) 
			return m;
		if (m == 0) 
			return n;
		int[][] d = new int[n + 1][m + 1];

		for (int i = 0; i <= n; i++)
		{
			d[i][0] = i;
		}

		for (int j = 0; j <= m; j++)
		{
			d[0][j] = j;
		}

		for (int i = 1; i <= n; i++)
		{
			char s_i = s.charAt(i - 1);

			for (int j = 1; j <= m; j++)
			{
				char t_j = t.charAt(j - 1);
				int cost;
				if (s_i == t_j)
				{
					cost = 0;
				}
				else
				{
					cost = 1;
				}

				d[i][j] = min(d[(i - 1)][j] + 1, d[i][(j - 1)] + 1, d[(i - 1)][(j - 1)] + 
						cost);
			}

		}

		return d[n][m];
	}

	public static float similarity(String s, String t)
	{
		double len = s.length() > t.length() ? s.length() : t.length();

		int d = ld(s, t);

		return (float)((len - d) / len);
	}
}