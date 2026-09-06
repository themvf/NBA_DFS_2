/* eslint-disable react-hooks/purity -- Dynamic page records snapshot age at request time. */
import TeamContext from './team-context';
export const dynamic='force-dynamic';
export default function Page(){return <TeamContext viewedAt={Date.now()}/>;}
