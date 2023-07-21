start = tokenit:(games1 / games2)+ 

games1 = '['"event"i Separator strings']' Separator 
        '['"site"i Separator strings']' Separator
        '['"date"i Separator strings']' Separator
        '['"round"i Separator strings']' Separator
        '['"white"i Separator strings']' Separator
        '['"black"i Separator strings']' Separator
        '['"result"i Separator strings']' Separator
        '['"whiteelo"i Separator strings']' Separator
        '['"blackelo"i Separator strings']' Separator
        '['"eco"i Separator strings']' Separator
        moves

games2 = '['"event"i Separator strings']' Separator 
        '['"site"i Separator strings']' Separator
        '['"date"i Separator strings']' Separator
        '['"round"i Separator strings']' Separator
        '['"white"i Separator strings']' Separator
        '['"black"i Separator strings']' Separator
        '['"result"i Separator strings']' Separator
        '['"whiteelo"i Separator strings']' Separator
        '['"blackelo"i Separator strings']' Separator
        moves

moves = char:[-OKQRBNa-hx0-9+!?#=\/. \t\r\n]* { return char.join(''); }
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
