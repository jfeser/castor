open Base
open Base.Polymorphic_compare

module MyList = struct
  include List

  let all_equal_exn : 'a list -> 'a =
    function
    | [] -> failwith "Empty list."
    | (x::xs) -> if List.for_all xs ~f:(fun x' -> x = x') then x else
        failwith "Not all elements equal."
end
module List = MyList
