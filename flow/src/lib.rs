pub mod first_ten;

pub mod first_ten_distributed;

use hydroflow_plus::futures::stream::Stream;
use hydroflow_plus::scheduled::graph::Hydroflow;
use hydroflow_plus::tokio::sync::mpsc::UnboundedSender;
use hydroflow_plus::tokio_stream::wrappers::UnboundedReceiverStream;
use stageleft::{q, Quoted, RuntimeData};

use hydroflow_plus::{FlowBuilder, Location, MultiGraph};

// #[stageleft::entry(UnboundedReceiverStream<String>)]
pub fn voting<'a, S: Stream<Item = String> + Unpin + 'a>(
    flow: &'a FlowBuilder<'a, MultiGraph>,
    followers_stream: RuntimeData<S>,
    output: RuntimeData<&'a UnboundedSender<String>>,
    subgraph_id: RuntimeData<usize>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let leader_process = flow.process(&());

    let source = leader_process.source_stream(followers_stream);

    let tick_aggregated = source.all_ticks();

    let has_no = tick_aggregated
        .filter(q!(|vote: &String| vote == "no"))
        .fold(q!(|| false), q!(|acc, _| *acc = true));

    has_no
        .map(q!(|has_no| if has_no { "no" } else { "yes" }))
        .for_each(q!(|final_result| {
            output.send(final_result.to_string()).unwrap();
        }));

    flow.build(subgraph_id)
}

mod tests {
    use super::*;
    use hydroflow_plus::futures::stream::StreamExt;
    use hydroflow_plus::runtime::RuntimeData;
    use hydroflow_plus::tokio_stream::wrappers::UnboundedReceiverStream;
    use hydroflow_plus::util::{collect_ready, unbounded_channel};

    #[test]
    fn test_voting_with_no_vote() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (in_send, input) = unbounded_channel();
            let input_stream = RuntimeData::new(" ");

            let (out_send, mut out_recv) = unbounded_channel::<String>();
            let output_stream = RuntimeData::new(" ");

            let mut flow_builder = FlowBuilder::new();

            let voting_process = voting(
                &flow_builder,
                input_stream,
                output_stream,
                RuntimeData::new("0"),
            );

            in_send.send("yes".to_string()).unwrap();
            in_send.send("yes".to_string()).unwrap();
            in_send.send("no".to_string()).unwrap();
            in_send.send("yes".to_string()).unwrap();

            voting_process.run_tick();

            // Expect 'no' as the output because there's at least one 'no' vote
            assert_eq!(collect_ready::<Vec<_>, _>(&mut out_recv), vec!["no"]);
        });
    }

    #[test]
    fn test_voting_with_only_yes_votes() {
        let (in_send, input) = unbounded_channel();
        let (out, mut out_recv) = unbounded_channel();

        let mut voting_flow = super::voting(input, &out, 0);

        in_send.send("yes".to_string()).unwrap();
        in_send.send("yes".to_string()).unwrap();
        in_send.send("yes".to_string()).unwrap();

        voting_flow.run_tick();

        assert_eq!(collect_ready::<Vec<_>, _>(&mut out_recv), vec!["yes"]);
    }
}
