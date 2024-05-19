#pragma once

extern const char *COMPOP_NAME[];


/**
 * @brief 描述比较运算符
 * @ingroup SQLParser
 */
enum CompOp 
{
  EQUAL_TO,     
  LESS_EQUAL,   
  NOT_EQUAL,    
  LESS_THAN,    
  GREAT_EQUAL,  
  GREAT_THAN,   
  NO_OP,
  LIKE_OP,
  NOT_LIKE_OP,
};

