Select * From Sailors ORDER BY Sailors.C;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.C < 30 AND S.C = R.G AND R.H < B.D ORDER BY S.C;
Select * FROM Sailors S, Boats B, Reserves R, Something S2 WHERE S.B<70 AND B.D<70 AND R.G<70 AND S2.I=S.B;
Select * FROM Sailors S, Boats B, Reserves R, Something S2 WHERE S.A<500 AND S.A=B.D AND B.D=R.G AND R.G=S2.I;