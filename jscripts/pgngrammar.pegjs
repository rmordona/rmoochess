start = tokenit:movesall result OpSeparator

movesall = token:(moves Separator)*


moves = movea / moveb / movec / moved  // / movee
movea = integers1 OpSeparator move OpSeparator move OpSeparator strings
moveb = integers1 OpSeparator move OpSeparator move
movec = integers1 OpSeparator move OpSeparator strings move
moved = integers1 OpSeparator move
// moved = integers2 OpSeparator move
// movee = integers3 OpSeparator move
result = result1 / result2
result1 = integers1 OpSeparator move OpSeparator ( "1-0" / "0-1" / "1/2-1/2" )
result2 = ( "1-0" / "0-1" / "1/2-1/2" )
integers1 = numbers: [0-9]+'.' { return numbers.join(''); }
// integers2 = numbers: [0-9]+'...' { return numbers.join(''); }
// integers3 = numbers: [0-9]+'.(...)' { return numbers.join(''); }
move = ( eatpiece / eatpiece1 / eatpiece2 / eatpiece3 / movepiece1 / movepiece2 / movepiece3 / castle ) promote checkit exlam
castle = "O-O-O" / "O-O"
eatpiece = ( ".." / "(...)" )
eatpiece1 = targets:([KQRBN]?[a-h]?[1-8]?"x"[a-h]+[1-8]+) { return targets.join(''); }
eatpiece2 = targets:([KQRBN]?[a-h1-8]?"x"[a-h]+[1-8]+) { return targets.join(''); }
eatpiece3 = targets:([KQRBNa-h]?"x"[a-h]+[1-8]+) { return targets.join(''); }
movepiece1 = targets:([KQRBN]?[a-h]?[1-8]?[a-h]?[1-8]+) { return targets.join(''); }
movepiece2 = targets:([KQRBN]?[a-h]?[1-8]+) { return targets.join(''); }
movepiece3 = targets:([KQRBN]?[a-h]?[1-8]+) { return targets.join(''); }
promote = prm:([QRBN("(Q)")("(R)")("(B)")("(N)")(=Q)(=R)(=B)(=N)])* { return prm.join(''); }
checkit = chk:([\#\+])* { return chk.join(''); }
exlam = exl:([!?]*) { return exl.join(''); }

OpSeparator = spaces:[ \t\r\n]* { return spaces.join(''); }
Separator = [ \t\r\n]+

// stackoverflow solution 33947960 with modifications 
// to accommodate curly brackets and parenthesis
strings
  = '"' chars:DoubleStringCharacter* '"' { return chars.join(''); }
  / "'" chars:SingleStringCharacter* "'" { return chars.join(''); }
  / '{' chars:CurlyCharacter* '}' { return chars.join(''); }
  / '(' chars:ParenCharacter* ')' { return chars.join(''); }

DoubleStringCharacter
  = !('"' / "\\") char:. { return char; }
  / "\\" sequence:EscapeSequence { return sequence; }

SingleStringCharacter
  = !("'" / "\\") char:. { return char; }
  / "\\" sequence:EscapeSequence { return sequence; }

CurlyCharacter
  = !('{' / '}' / "\\") char:. { return char; }
  / "\\" sequence:EscapeSequence { return sequence; }

ParenCharacter
  = !('(' / ')' / "\\") char:. { return char; }
  / "\\" sequence:EscapeSequence { return sequence; }

EscapeSequence
  = "'"
  / '"'
  / "\\"
  / "b"  { return "\b";   }
  / "f"  { return "\f";   }
  / "n"  { return "\n";   }
  / "r"  { return "\r";   }
  / "t"  { return "\t";   }
  / "v"  { return "\x0B"; }
