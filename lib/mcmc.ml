include (val Log.make ~level:(Some Info) "castor-opt.mcmc")

module Random_choice = struct
  module T = struct
    type t = {
      mutable pairs : ((string * Ast.t) * bool) list;
      state : (Random.State.t[@sexp.opaque]); [@compare.ignore]
    }
    [@@deriving compare, sexp]
  end

  include T

  module C = struct
    include T
    include Comparable.Make (T)
  end

  let create ?(seed = 0) () =
    { pairs = []; state = Random.State.make [| seed |] }

  let rand rand n r =
    match
      List.Assoc.find ~equal:[%compare.equal: string * Ast.t] rand.pairs (n, r)
    with
    | Some v -> v
    | None ->
        rand.pairs <- ((n, r), true) :: rand.pairs;
        true

  let rec perturb rand =
    let i' = Random.State.int rand.state @@ List.length rand.pairs in
    {
      rand with
      pairs =
        List.mapi rand.pairs ~f:(fun i (k, v) ->
            if i = i' then (k, false) else (k, v));
    }

  let length r = List.length r.pairs
end

let run ?(max_time = Time.Span.of_min 10.0) eval =
  let start_time = Time.now () in
  let state = Random_choice.create () in
  let score = eval state in
  info (fun m -> m "Initial score %f" score);
  let rec loop state score =
    info (fun m -> m "State space size: %d" (Random_choice.length state));
    if Time.(Span.(diff (now ()) start_time > max_time)) then (
      info (fun m -> m "Out of time. Final score %f" score);
      (state, score) )
    else
      let state' = Random_choice.perturb state in
      let score' = eval state' in
      let h = Float.(min 1.0 (exp @@ (score - score'))) in
      let u = Random.float 1.0 in
      if Float.(u < h) then (
        info (fun m ->
            m "Transitioning. Old score %f, new score %f" score score');
        loop state' score' )
      else (
        info (fun m -> m "Staying. Old score %f, new score %f" score score');
        loop state score )
  in
  loop state score
